#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netdb.h>
#include <poll.h>
#include <stdatomic.h>
#include <time.h>

#define BUF_SIZE 4096
#define MAX_BIND 1024
#define MAX_CONNECTIONS (4096 - MAX_BIND)

#if MAX_BIND <= 0
#error "MAX_BIND must be > 0"
#endif
#if MAX_CONNECTIONS <= 0
#error "MAX_CONNECTIONS must be > 0"
#endif

typedef struct {
    char local_ip[64];
    int local_port;
    char remote_host[256];
    int remote_port;
} forward_spec;

typedef struct {
    int client_sock;
    int remote_sock;
    char conn_id[7]; // 6 chars + NUL
    char client_addr[64];
    char remote_addr[270];
    size_t bytes_c2r; // client -> remote
    size_t bytes_r2c; // remote -> client
    struct timespec start_ts; // CLOCK_MONOTONIC at accept time
    struct timespec last_activity_ts; // CLOCK_MONOTONIC, updated on any successful relay
} conn_pair;

static int g_log_no_time = 0;
static atomic_llong g_total_connections = 0; // successfully established pairs
static atomic_int g_active_connections = 0;

// Utility to print timestamped log
void log_msg(const char *fmt, ...) {
    va_list ap;
    if (!g_log_no_time) {
        char timestr[64];
        struct timespec ts;
        if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
            // Fallback (should be rare)
            ts.tv_sec = time(NULL);
            ts.tv_nsec = 0;
        }
        struct tm tm;
        localtime_r(&ts.tv_sec, &tm);
        strftime(timestr, sizeof(timestr), "%Y-%m-%d %H:%M:%S", &tm);
        long ms = ts.tv_nsec / 1000000L;
        printf("[%s.%03ld] ", timestr, ms);
    }
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf("\n");
    fflush(stdout);
}

// Utility to print timestamped log with connection id prefix
void log_conn(const char *conn_id, const char *fmt, ...) {
    va_list ap;
    if (!g_log_no_time) {
        char timestr[64];
        struct timespec ts;
        if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
            ts.tv_sec = time(NULL);
            ts.tv_nsec = 0;
        }
        struct tm tm;
        localtime_r(&ts.tv_sec, &tm);
        strftime(timestr, sizeof(timestr), "%Y-%m-%d %H:%M:%S", &tm);
        long ms = ts.tv_nsec / 1000000L;
        printf("[%s.%03ld] ", timestr, ms);
    }
    printf("[%s] ", (conn_id && conn_id[0]) ? conn_id : "??????");
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf("\n");
    fflush(stdout);
}

static void gen_conn_id(char out[7]) {
    static const char charset[] =
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "0123456789";
    // 62 chars

    int fd = open("/dev/urandom", O_RDONLY);
    unsigned char b;
    for (int i = 0; i < 6; i++) {
        int ok = 0;
        if (fd >= 0) {
            // rejection sample to avoid modulo bias (largest multiple of 62 under 256 is 248)
            for (int tries = 0; tries < 128; tries++) {
                ssize_t r = read(fd, &b, 1);
                if (r != 1) break;
                if (b < 248) { out[i] = charset[b % 62]; ok = 1; break; }
            }
        }
        if (!ok) {
            // fallback (not crypto-strong, but keeps behavior working)
            out[i] = charset[(unsigned)(rand() % 62)];
        }
    }
    if (fd >= 0) close(fd);
    out[6] = 0;
}

static void *stats_thread_main(void *arg) {
    (void)arg;
    while (1) {
        sleep(10);
        long long total = atomic_load(&g_total_connections);
        int active = atomic_load(&g_active_connections);
        log_msg("stats: total_connections=%lld active_connections=%d", total, active);
    }
    return NULL;
}

// Connect to remote host
int connect_remote(const char *host, int port) {
    struct addrinfo hints, *res;
    int sock;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    char portstr[16];
    snprintf(portstr, sizeof(portstr), "%d", port);

    if (getaddrinfo(host, portstr, &hints, &res) != 0) {
        perror("getaddrinfo");
        return -1;
    }

    sock = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (sock < 0) { perror("socket"); freeaddrinfo(res); return -1; }

    if (connect(sock, res->ai_addr, res->ai_addrlen) < 0) {
        perror("connect");
        close(sock);
        freeaddrinfo(res);
        return -1;
    }

    freeaddrinfo(res);
    return sock;
}

// Parse -L argument
int parse_spec(const char *arg, forward_spec *spec) {
    char tmp[512];
    strncpy(tmp, arg, sizeof(tmp)-1);
    tmp[sizeof(tmp)-1] = 0;

    char *at = strchr(tmp, '@');
    if (!at) return -1;
    *at = 0;
    char *local = tmp;
    char *remote = at+1;

    // local [ip:]port
    // If ip is omitted, default to 127.0.0.1 so specs like "80@host:80" work.
    char *colon = strchr(local, ':');
    if (colon) {
        *colon = 0;
        const char *lip = local;
        if (lip[0] == '\0') lip = "127.0.0.1";
        strncpy(spec->local_ip, lip, sizeof(spec->local_ip)-1);
        spec->local_ip[sizeof(spec->local_ip)-1] = 0;
        spec->local_port = atoi(colon+1);
    } else {
        strncpy(spec->local_ip, "127.0.0.1", sizeof(spec->local_ip)-1);
        spec->local_ip[sizeof(spec->local_ip)-1] = 0;
        spec->local_port = atoi(local);
    }
    if (spec->local_port <=0 || spec->local_port>65535) return -1;

    // remote host:port
    colon = strchr(remote, ':');
    if (colon) {
        *colon = 0;
        strncpy(spec->remote_host, remote, sizeof(spec->remote_host)-1);
        spec->remote_host[sizeof(spec->remote_host)-1] = 0;
        spec->remote_port = atoi(colon+1);
    } else {
        strncpy(spec->remote_host, remote, sizeof(spec->remote_host)-1);
        spec->remote_host[sizeof(spec->remote_host)-1] = 0;
        spec->remote_port = spec->local_port;
    }
    if (spec->remote_port <=0 || spec->remote_port>65535) return -1;

    return 0;
}

// Setup listening socket
int setup_listener(forward_spec *spec) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) { perror("socket"); return -1; }

    int opt = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in laddr;
    laddr.sin_family = AF_INET;
    laddr.sin_port = htons(spec->local_port);
    if (inet_pton(AF_INET, spec->local_ip, &laddr.sin_addr)<=0) {
        perror("inet_pton"); close(sock); return -1;
    }

    if (bind(sock, (struct sockaddr*)&laddr, sizeof(laddr))<0) {
        perror("bind"); close(sock); return -1;
    }

    if (listen(sock, 16)<0) { perror("listen"); close(sock); return -1; }

    log_msg("Listening on %s:%d -> %s:%d",
           spec->local_ip, spec->local_port,
           spec->remote_host, spec->remote_port);
    return sock;
}

// Format bytes to human readable
void human_bytes(size_t bytes, char *buf, size_t buflen) {
    const char *units[]={"B","KB","MB","GB"};
    int i=0;
    double b=bytes;
    while(b>=1024 && i<3){ b/=1024; i++; }
    snprintf(buf, buflen, "%.2f %s", b, units[i]);
}

// Compute elapsed milliseconds between two timespecs (assumes end >= start).
static long long elapsed_ms(const struct timespec *start, const struct timespec *end) {
    long long sec = (long long)end->tv_sec - (long long)start->tv_sec;
    long long nsec = (long long)end->tv_nsec - (long long)start->tv_nsec;
    if (nsec < 0) { sec--; nsec += 1000000000LL; }
    return sec * 1000LL + nsec / 1000000LL;
}

int main(int argc, char **argv) {
    if (argc<2) {
        fprintf(stderr,"Usage: %s [-log-no-time] [-idle-timeout-secs N] -L [local_ip:]local_port@remote_host[:remote_port] [-L ...]\n", argv[0]);
        return 1;
    }

    long long idle_timeout_secs = -1; // -1 disables idle timeout

    forward_spec *specs = calloc((size_t)MAX_BIND, sizeof(*specs));
    if (!specs) { perror("calloc specs"); return 1; }
    int nbind = 0;

    for (int i=1;i<argc;i++) {
        if (strcmp(argv[i], "-log-no-time")==0) {
            g_log_no_time = 1;
        } else if (strcmp(argv[i], "-idle-timeout-secs")==0) {
            if (i+1>=argc) { fprintf(stderr,"-idle-timeout-secs requires an integer argument\n"); return 1; }
            char *end = NULL;
            errno = 0;
            long long v = strtoll(argv[i+1], &end, 10);
            if (errno != 0 || end == argv[i+1] || *end != '\0') {
                fprintf(stderr,"Invalid -idle-timeout-secs value: %s\n", argv[i+1]);
                return 1;
            }
            if (v < -1) {
                fprintf(stderr,"-idle-timeout-secs must be -1 (disabled) or >= 0\n");
                return 1;
            }
            idle_timeout_secs = v;
            i++; // consume argument
        } else if (strcmp(argv[i], "-L")==0) {
            if (i+1>=argc) { fprintf(stderr,"-L requires an argument\n"); return 1; }
            if (nbind>=MAX_BIND) { fprintf(stderr,"Too many -L\n"); return 1; }
            if (parse_spec(argv[i+1],&specs[nbind])<0) {
                fprintf(stderr,"Invalid -L spec: %s\n", argv[i+1]); return 1;
            }
            nbind++;
            i++; // consume spec argument
        } else if (strncmp(argv[i], "-L", 2)==0) {
            fprintf(stderr,"Invalid argument: %s (use: -L <spec>)\n", argv[i]);
            return 1;
        }
    }
    if (nbind==0) { fprintf(stderr,"No -L specified\n"); return 1; }

    // Seed rand() for gen_conn_id() fallback path.
    srand((unsigned)(time(NULL) ^ getpid()));

    int *listen_socks = calloc((size_t)MAX_BIND, sizeof(*listen_socks));
    if (!listen_socks) { perror("calloc listen_socks"); free(specs); return 1; }
    int nlisten = 0;
    for (int i=0;i<nbind;i++) {
        int sock = setup_listener(&specs[i]);
        if (sock<0) {
            for (int j=0;j<nlisten;j++) close(listen_socks[j]);
            free(listen_socks);
            free(specs);
            return 1;
        }
        listen_socks[i] = sock;
        nlisten++;
    }

    conn_pair *conns = calloc((size_t)MAX_CONNECTIONS, sizeof(*conns));
    if (!conns) {
        perror("calloc conns");
        for (int j=0;j<nlisten;j++) close(listen_socks[j]);
        free(listen_socks);
        free(specs);
        return 1;
    }
    int nconns = 0;

    struct pollfd *fds = calloc((size_t)(MAX_CONNECTIONS*2 + MAX_BIND), sizeof(*fds));
    if (!fds) {
        perror("calloc fds");
        free(conns);
        for (int j=0;j<nlisten;j++) close(listen_socks[j]);
        free(listen_socks);
        free(specs);
        return 1;
    }
    int nfds;

    // Start stats thread
    pthread_t stats_thread;
    if (pthread_create(&stats_thread, NULL, stats_thread_main, NULL) == 0) {
        pthread_detach(stats_thread);
    } else {
        log_msg("warning: failed to start stats thread");
    }

    while (1) {
        nfds = 0;
        // add listening sockets
        for (int i=0;i<nbind;i++) {
            fds[nfds].fd = listen_socks[i];
            fds[nfds].events = POLLIN;
            nfds++;
        }
        // add active connections
        for (int i=0;i<nconns;i++) {
            fds[nfds].fd = conns[i].client_sock;
            fds[nfds].events = POLLIN;
            nfds++;
            fds[nfds].fd = conns[i].remote_sock;
            fds[nfds].events = POLLIN;
            nfds++;
        }

        int poll_timeout_ms = -1;
        if (idle_timeout_secs >= 0 && nconns > 0) {
            const long long timeout_ms = idle_timeout_secs * 1000LL;
            struct timespec now_ts;
            clock_gettime(CLOCK_MONOTONIC, &now_ts);

            long long min_rem_ms = LLONG_MAX;
            for (int i=0;i<nconns;i++) {
                long long idle_ms = elapsed_ms(&conns[i].last_activity_ts, &now_ts);
                long long rem = timeout_ms - idle_ms;
                if (rem < 0) rem = 0;
                if (rem < min_rem_ms) min_rem_ms = rem;
            }
            if (min_rem_ms == LLONG_MAX) min_rem_ms = 0;
            poll_timeout_ms = (min_rem_ms > INT_MAX) ? INT_MAX : (int)min_rem_ms;
        }

        int ret = poll(fds,nfds,poll_timeout_ms);
        if (ret<0) { perror("poll"); continue; }

        // listen sockets
        for (int i=0;i<nbind;i++) {
            if (fds[i].revents & POLLIN) {
                struct sockaddr_in caddr;
                socklen_t clen = sizeof(caddr);
                int client_sock = accept(listen_socks[i], (struct sockaddr*)&caddr, &clen);
                if (client_sock<0) { perror("accept"); continue; }

                char cid[7];
                gen_conn_id(cid);

                char caddrstr[64];
                inet_ntop(AF_INET, &caddr.sin_addr, caddrstr, sizeof(caddrstr));
                log_conn(cid, "client connected from %s:%d", caddrstr, ntohs(caddr.sin_port));

                int remote_sock = connect_remote(specs[i].remote_host, specs[i].remote_port);
                if (remote_sock<0) {
                    log_conn(cid, "remote connect failed %s:%d", specs[i].remote_host, specs[i].remote_port);
                    close(client_sock);
                    continue;
                }
                log_conn(cid, "remote connected %s:%d", specs[i].remote_host, specs[i].remote_port);

                if (nconns>=MAX_CONNECTIONS) {
                    log_conn(cid, "Too many connections, dropping client %s", caddrstr);
                    close(client_sock);
                    close(remote_sock);
                    continue;
                }
                conns[nconns].client_sock = client_sock;
                conns[nconns].remote_sock = remote_sock;
                strncpy(conns[nconns].conn_id, cid, sizeof(conns[nconns].conn_id)-1);
                conns[nconns].conn_id[sizeof(conns[nconns].conn_id)-1] = 0;
                strncpy(conns[nconns].client_addr, caddrstr, sizeof(conns[nconns].client_addr)-1);
                conns[nconns].client_addr[sizeof(conns[nconns].client_addr)-1] = 0;
                snprintf(conns[nconns].remote_addr, sizeof(conns[nconns].remote_addr),"%s:%d",
                        specs[i].remote_host, specs[i].remote_port);
                conns[nconns].bytes_c2r = 0;
                conns[nconns].bytes_r2c = 0;
                clock_gettime(CLOCK_MONOTONIC, &conns[nconns].start_ts);
                conns[nconns].last_activity_ts = conns[nconns].start_ts;
                nconns++;
                atomic_fetch_add(&g_total_connections, 1);
                atomic_fetch_add(&g_active_connections, 1);
            }
        }

        // active connections
        int i=0;
        while(i<nconns) {
            int closed = 0;
            const char *close_reason = NULL;
            char buf[BUF_SIZE];
            int n;

            // client -> remote
            if (fds[nbind + i*2].revents & POLLIN) {
                n = read(conns[i].client_sock, buf, sizeof(buf));
                if (n<=0) { log_conn(conns[i].conn_id, "client disconnected"); closed=1; close_reason="client disconnect"; }
                else if (write(conns[i].remote_sock, buf, n)!=n) { log_conn(conns[i].conn_id, "write to remote failed"); closed=1; close_reason="write to remote failed"; }
                else { conns[i].bytes_c2r += (size_t)n; clock_gettime(CLOCK_MONOTONIC, &conns[i].last_activity_ts); }
            }

            // remote -> client
            if (!closed && (fds[nbind + i*2 +1].revents & POLLIN)) {
                n = read(conns[i].remote_sock, buf, sizeof(buf));
                if (n<=0) { log_conn(conns[i].conn_id, "remote disconnected"); closed=1; close_reason="remote disconnect"; }
                else if (write(conns[i].client_sock, buf, n)!=n) { log_conn(conns[i].conn_id, "write to client failed"); closed=1; close_reason="write to client failed"; }
                else { conns[i].bytes_r2c += (size_t)n; clock_gettime(CLOCK_MONOTONIC, &conns[i].last_activity_ts); }
            }

            // idle timeout (no traffic in either direction)
            if (!closed && idle_timeout_secs >= 0) {
                const long long timeout_ms = idle_timeout_secs * 1000LL;
                struct timespec now_ts;
                clock_gettime(CLOCK_MONOTONIC, &now_ts);
                long long idle_ms = elapsed_ms(&conns[i].last_activity_ts, &now_ts);
                if (idle_ms > timeout_ms) {
                    log_conn(conns[i].conn_id, "idle timeout");
                    closed = 1;
                    close_reason = "idle timeout";
                }
            }

            if (closed) {
                close(conns[i].client_sock);
                close(conns[i].remote_sock);
                char hbuf[64], rbuf[64];
                human_bytes(conns[i].bytes_c2r,hbuf,sizeof(hbuf));
                human_bytes(conns[i].bytes_r2c,rbuf,sizeof(rbuf));
                struct timespec end_ts;
                clock_gettime(CLOCK_MONOTONIC, &end_ts);
                long long ms = elapsed_ms(&conns[i].start_ts, &end_ts);
                log_conn(conns[i].conn_id, "connection closed, uptime %.3fs, bytes: in %s, out %s%s%s",
                         (double)ms / 1000.0, hbuf, rbuf,
                         close_reason ? ", reason: " : "", close_reason ? close_reason : "");
                atomic_fetch_sub(&g_active_connections, 1);

                if (i<nconns-1) conns[i]=conns[nconns-1];
                nconns--;
            } else i++;
        }
    }

    // Unreachable, but keep tidy for future changes.
    // free(fds);
    // free(conns);
    // for (int j=0;j<nlisten;j++) close(listen_socks[j]);
    // free(listen_socks);
    // free(specs);
    return 0;
}