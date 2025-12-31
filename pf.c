#define _GNU_SOURCE
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
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netdb.h>
#include <stdatomic.h>
#include <time.h>

#define BUF_SIZE (128 * 1024)
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
    int client_read_open; // whether client can still send data to us (read side open)
    int remote_read_open; // whether remote can still send data to us (read side open)
    int pending_shutdown_remote_wr; // shutdown(remote, SHUT_WR) once c2r pipe drains
    int pending_shutdown_client_wr; // shutdown(client, SHUT_WR) once r2c pipe drains

    int p_c2r[2]; // pipe for client->remote: [0]=read end, [1]=write end
    int p_r2c[2]; // pipe for remote->client
    size_t c2r_in_pipe;
    size_t r2c_in_pipe;
    int c2r_ingress_blocked; // socket->pipe would block (pipe full)
    int r2c_ingress_blocked; // socket->pipe would block (pipe full)
    int watching_c2r_pw; // pipe write end is registered in epoll
    int watching_r2c_pw;
} conn_pair;

typedef struct {
    int kind; // 1=listen, 2=client_sock, 3=remote_sock
    int idx;  // listen index or connection index
} ep_ctx;

enum { EP_KIND_LISTEN = 1, EP_KIND_CLIENT = 2, EP_KIND_REMOTE = 3, EP_KIND_C2R_PW = 4, EP_KIND_R2C_PW = 5 };

static int g_log_no_time = 0;
static int g_log_no_stats = 0;
static int g_stats_interval_secs = 30;
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

static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) return -1;
    return 0;
}

static int make_pipe_nb(int p[2]) {
    if (pipe2(p, O_NONBLOCK | O_CLOEXEC) != 0) return -1;
    return 0;
}

static void maybe_set_pipe_size(const char *cid, int pipe_fd, int size) {
#ifdef F_SETPIPE_SZ
    if (size <= 0) return;
    if (fcntl(pipe_fd, F_SETPIPE_SZ, size) < 0) {
        // Best-effort; often limited by permissions / pipe-max-size.
        if (cid) log_conn(cid, "warning: F_SETPIPE_SZ(%d) failed: %s", size, strerror(errno));
    }
#else
    (void)cid; (void)pipe_fd; (void)size;
#endif
}

static uint32_t conn_ep_events_client(const conn_pair *c) {
    uint32_t ev = EPOLLRDHUP | EPOLLHUP | EPOLLERR;
    if (c->client_read_open && !c->c2r_ingress_blocked) ev |= EPOLLIN;
    if (c->r2c_in_pipe > 0) ev |= EPOLLOUT;
    return ev;
}

static uint32_t conn_ep_events_remote(const conn_pair *c) {
    uint32_t ev = EPOLLRDHUP | EPOLLHUP | EPOLLERR;
    if (c->remote_read_open && !c->r2c_ingress_blocked) ev |= EPOLLIN;
    if (c->c2r_in_pipe > 0) ev |= EPOLLOUT;
    return ev;
}

static void *stats_thread_main(void *arg) {
    int interval_secs = *(int *)arg;
    free(arg);
    if (interval_secs <= 0) interval_secs = 30;
    while (1) {
        sleep((unsigned)interval_secs);
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

static void print_usage(FILE *out, const char *prog) {
    fprintf(out,
            "Usage:\n"
            "  %s [-h|--help] [-log-no-time] [-log-no-stats] [-stats-interval-secs N] [-idle-timeout-secs N] -L [local_ip:]local_port@remote_host[:remote_port] [-L ...]\n"
            "  %s [-h|--help] [-log-no-time] [-log-no-stats] [-stats-interval-secs N] [-idle-timeout-secs N]\n"
            "     (if no -L is provided, specs are loaded from ./pf.conf)\n"
            "\n"
            "Options:\n"
            "  -h, --help              Show this help\n"
            "  -L <spec>               Add a TCP forward rule\n"
            "                           spec format: [local_ip:]local_port@remote_host[:remote_port]\n"
            "                           local_ip defaults to 127.0.0.1 if omitted\n"
            "                           remote_port defaults to local_port if omitted\n"
            "  pf.conf                 If no -L is provided, pf reads specs from ./pf.conf\n"
            "                           One spec per line, up to MAX_BIND lines.\n"
            "  -idle-timeout-secs N    Close a connection if no traffic flows in either direction for N seconds\n"
            "                           Default: -1 (disabled)\n"
            "  -log-no-time            Omit pf's own timestamp prefix (useful under systemd/journald)\n"
            "  -log-no-stats           Disable periodic stats logging\n"
            "  -stats-interval-secs N  Stats logging interval in seconds\n"
            "                           Default: 30\n"
            "\n"
            "Examples:\n"
            "  %s -L 0.0.0.0:23@mama.wushilin.net:22222\n"
            "  %s -idle-timeout-secs 5 -log-no-time -L 80@target-host.com:80\n",
            prog, prog, prog, prog);
}

static int load_specs_from_file(const char *path, forward_spec *specs, int *nbind) {
    FILE *f = fopen(path, "r");
    if (!f) return -1;

    char line[1024];
    int lineno = 0;
    while (fgets(line, sizeof(line), f)) {
        lineno++;
        // trim leading whitespace
        char *s = line;
        while (*s == ' ' || *s == '\t' || *s == '\r' || *s == '\n') s++;
        // strip trailing newline/whitespace
        char *end = s + strlen(s);
        while (end > s && (end[-1] == '\n' || end[-1] == '\r' || end[-1] == ' ' || end[-1] == '\t')) end--;
        *end = 0;

        if (*s == 0) continue;          // skip empty
        if (*s == '#') continue;        // skip comment lines

        if (*nbind >= MAX_BIND) {
            fprintf(stderr, "Too many specs in %s (max %d)\n", path, MAX_BIND);
            fclose(f);
            return -2;
        }
        if (parse_spec(s, &specs[*nbind]) < 0) {
            fprintf(stderr, "Invalid spec in %s line %d: %s\n", path, lineno, s);
            fclose(f);
            return -3;
        }
        (*nbind)++;
    }
    fclose(f);
    return 0;
}

int main(int argc, char **argv) {
    long long idle_timeout_secs = -1; // -1 disables idle timeout

    forward_spec *specs = calloc((size_t)MAX_BIND, sizeof(*specs));
    if (!specs) { perror("calloc specs"); return 1; }
    int nbind = 0;

    for (int i=1;i<argc;i++) {
        if (strcmp(argv[i], "-h")==0 || strcmp(argv[i], "--help")==0) {
            print_usage(stdout, argv[0]);
            return 0;
        } else if (strcmp(argv[i], "-log-no-time")==0) {
            g_log_no_time = 1;
        } else if (strcmp(argv[i], "-log-no-stats")==0) {
            g_log_no_stats = 1;
        } else if (strcmp(argv[i], "-stats-interval-secs")==0) {
            if (i+1>=argc) { fprintf(stderr,"-stats-interval-secs requires an integer argument\n"); return 1; }
            char *end = NULL;
            errno = 0;
            long long v = strtoll(argv[i+1], &end, 10);
            if (errno != 0 || end == argv[i+1] || *end != '\0' || v <= 0 || v > INT_MAX) {
                fprintf(stderr,"Invalid -stats-interval-secs value: %s\n", argv[i+1]);
                return 1;
            }
            g_stats_interval_secs = (int)v;
            i++; // consume argument
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
    if (nbind==0) {
        // If no -L is provided, load specs from ./pf.conf in the current working directory.
        if (load_specs_from_file("./pf.conf", specs, &nbind) != 0 || nbind == 0) {
            fprintf(stderr, "No -L specified and no usable specs found in ./pf.conf\n\n");
            print_usage(stderr, argv[0]);
            return 1;
        }
    }

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

    // free-list of connection slots
    int *free_idxs = malloc((size_t)MAX_CONNECTIONS * sizeof(*free_idxs));
    if (!free_idxs) {
        perror("malloc free_idxs");
        free(conns);
        for (int j=0;j<nlisten;j++) close(listen_socks[j]);
        free(listen_socks);
        free(specs);
        return 1;
    }
    int free_top = 0;
    for (int i=0;i<MAX_CONNECTIONS;i++) free_idxs[free_top++] = i;
    int nconns = 0;

    int epfd = epoll_create1(EPOLL_CLOEXEC);
    if (epfd < 0) {
        perror("epoll_create1");
        free(free_idxs);
        free(conns);
        for (int j=0;j<nlisten;j++) close(listen_socks[j]);
        free(listen_socks);
        free(specs);
        return 1;
    }

    ep_ctx *listen_ctxs = calloc((size_t)nbind, sizeof(*listen_ctxs));
    // per-conn contexts: client, remote, c2r pipe write, r2c pipe write
    ep_ctx *conn_ctxs = calloc((size_t)MAX_CONNECTIONS * 4, sizeof(*conn_ctxs));
    if (!listen_ctxs || !conn_ctxs) {
        perror("calloc ep_ctx");
        close(epfd);
        free(conn_ctxs);
        free(listen_ctxs);
        free(free_idxs);
        free(conns);
        for (int j=0;j<nlisten;j++) close(listen_socks[j]);
        free(listen_socks);
        free(specs);
        return 1;
    }

    // Register listening sockets with epoll
    for (int i=0;i<nbind;i++) {
        // make listener nonblocking so we can accept in a loop
        set_nonblocking(listen_socks[i]);

        listen_ctxs[i].kind = EP_KIND_LISTEN;
        listen_ctxs[i].idx = i;
        struct epoll_event ev;
        memset(&ev, 0, sizeof(ev));
        ev.events = EPOLLIN;
        ev.data.ptr = &listen_ctxs[i];
        if (epoll_ctl(epfd, EPOLL_CTL_ADD, listen_socks[i], &ev) != 0) {
            perror("epoll_ctl ADD listen");
            return 1;
        }
    }

    // Start stats thread
    if (!g_log_no_stats) {
        pthread_t stats_thread;
        int *interval_arg = malloc(sizeof(*interval_arg));
        if (!interval_arg) {
            log_msg("warning: failed to start stats thread (malloc)");
        } else {
            *interval_arg = g_stats_interval_secs;
            if (pthread_create(&stats_thread, NULL, stats_thread_main, interval_arg) == 0) {
                pthread_detach(stats_thread);
            } else {
                free(interval_arg);
                log_msg("warning: failed to start stats thread (pthread_create)");
            }
        }
    }

    // epoll event buffer
    struct epoll_event *evs = calloc(1024, sizeof(*evs));
    if (!evs) {
        perror("calloc epoll events");
        return 1;
    }

    while (1) {
        int timeout_ms = -1;
        if (idle_timeout_secs >= 0 && nconns > 0) {
            const long long tms = idle_timeout_secs * 1000LL;
            struct timespec now_ts;
            clock_gettime(CLOCK_MONOTONIC, &now_ts);
            long long min_rem = LLONG_MAX;
            for (int idx=0; idx<MAX_CONNECTIONS; idx++) {
                if (conns[idx].client_sock == 0 && conns[idx].remote_sock == 0) continue;
                long long idle_ms = elapsed_ms(&conns[idx].last_activity_ts, &now_ts);
                long long rem = tms - idle_ms;
                if (rem < 0) rem = 0;
                if (rem < min_rem) min_rem = rem;
            }
            if (min_rem == LLONG_MAX) min_rem = 0;
            timeout_ms = (min_rem > INT_MAX) ? INT_MAX : (int)min_rem;
        }

        int n = epoll_wait(epfd, evs, 1024, timeout_ms);
        if (n < 0) {
            if (errno == EINTR) continue;
            perror("epoll_wait");
            continue;
        }

        // track which conns need pumping this round
        unsigned char *to_pump = calloc((size_t)MAX_CONNECTIONS, 1);
        if (!to_pump) { perror("calloc to_pump"); return 1; }

        for (int e=0; e<n; e++) {
            ep_ctx *ctx = (ep_ctx *)evs[e].data.ptr;
            if (!ctx) continue;
            if (ctx->kind == EP_KIND_LISTEN) {
                int li = ctx->idx;
                while (1) {
                    struct sockaddr_in caddr;
                    socklen_t clen = sizeof(caddr);
                    int client_sock = accept4(listen_socks[li], (struct sockaddr*)&caddr, &clen, SOCK_NONBLOCK | SOCK_CLOEXEC);
                    if (client_sock < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        perror("accept4");
                        break;
                    }

                    char cid[7];
                    gen_conn_id(cid);
                    char caddrstr[64];
                    inet_ntop(AF_INET, &caddr.sin_addr, caddrstr, sizeof(caddrstr));
                    log_conn(cid, "client connected from %s:%d", caddrstr, ntohs(caddr.sin_port));

                    int remote_sock = connect_remote(specs[li].remote_host, specs[li].remote_port);
                    if (remote_sock < 0) {
                        log_conn(cid, "remote connect failed %s:%d", specs[li].remote_host, specs[li].remote_port);
                        close(client_sock);
                        continue;
                    }
                    set_nonblocking(remote_sock);
                    log_conn(cid, "remote connected %s:%d", specs[li].remote_host, specs[li].remote_port);

                    if (free_top <= 0) {
                        log_conn(cid, "Too many connections, dropping client %s", caddrstr);
                        close(client_sock);
                        close(remote_sock);
                        continue;
                    }
                    int idx = free_idxs[--free_top];
                    memset(&conns[idx], 0, sizeof(conns[idx]));

                    int p_c2r[2] = {-1, -1};
                    int p_r2c[2] = {-1, -1};
                    if (make_pipe_nb(p_c2r) != 0 || make_pipe_nb(p_r2c) != 0) {
                        log_conn(cid, "pipe2 failed");
                        if (p_c2r[0] >= 0) { close(p_c2r[0]); close(p_c2r[1]); }
                        if (p_r2c[0] >= 0) { close(p_r2c[0]); close(p_r2c[1]); }
                        close(client_sock);
                        close(remote_sock);
                        free_idxs[free_top++] = idx;
                        continue;
                    }
                    // Best-effort: increase pipe capacity (helps burst throughput / fewer wakeups)
                    maybe_set_pipe_size(cid, p_c2r[0], BUF_SIZE * 2);
                    maybe_set_pipe_size(cid, p_r2c[0], BUF_SIZE * 2);

                    conns[idx].client_sock = client_sock;
                    conns[idx].remote_sock = remote_sock;
                    strncpy(conns[idx].conn_id, cid, sizeof(conns[idx].conn_id)-1);
                    conns[idx].conn_id[sizeof(conns[idx].conn_id)-1] = 0;
                    strncpy(conns[idx].client_addr, caddrstr, sizeof(conns[idx].client_addr)-1);
                    conns[idx].client_addr[sizeof(conns[idx].client_addr)-1] = 0;
                    snprintf(conns[idx].remote_addr, sizeof(conns[idx].remote_addr), "%s:%d",
                             specs[li].remote_host, specs[li].remote_port);
                    conns[idx].p_c2r[0] = p_c2r[0];
                    conns[idx].p_c2r[1] = p_c2r[1];
                    conns[idx].p_r2c[0] = p_r2c[0];
                    conns[idx].p_r2c[1] = p_r2c[1];
                    clock_gettime(CLOCK_MONOTONIC, &conns[idx].start_ts);
                    conns[idx].last_activity_ts = conns[idx].start_ts;
                    conns[idx].client_read_open = 1;
                    conns[idx].remote_read_open = 1;
                    conns[idx].watching_c2r_pw = 0;
                    conns[idx].watching_r2c_pw = 0;

                    // Register client and remote sockets with epoll (pipes are not polled; we rely on splice EAGAIN)
                    conn_ctxs[idx*4].kind = EP_KIND_CLIENT;
                    conn_ctxs[idx*4].idx = idx;
                    conn_ctxs[idx*4+1].kind = EP_KIND_REMOTE;
                    conn_ctxs[idx*4+1].idx = idx;
                    conn_ctxs[idx*4+2].kind = EP_KIND_C2R_PW;
                    conn_ctxs[idx*4+2].idx = idx;
                    conn_ctxs[idx*4+3].kind = EP_KIND_R2C_PW;
                    conn_ctxs[idx*4+3].idx = idx;

                    struct epoll_event evc;
                    memset(&evc, 0, sizeof(evc));
                    evc.events = conn_ep_events_client(&conns[idx]);
                    evc.data.ptr = &conn_ctxs[idx*4];
                    if (epoll_ctl(epfd, EPOLL_CTL_ADD, client_sock, &evc) != 0) {
                        perror("epoll_ctl ADD client");
                    }
                    struct epoll_event evr;
                    memset(&evr, 0, sizeof(evr));
                    evr.events = conn_ep_events_remote(&conns[idx]);
                    evr.data.ptr = &conn_ctxs[idx*4+1];
                    if (epoll_ctl(epfd, EPOLL_CTL_ADD, remote_sock, &evr) != 0) {
                        perror("epoll_ctl ADD remote");
                    }

                    nconns++;
                    atomic_fetch_add(&g_total_connections, 1);
                    atomic_fetch_add(&g_active_connections, 1);
                }
            } else if (ctx->kind == EP_KIND_CLIENT || ctx->kind == EP_KIND_REMOTE) {
                if (ctx->idx >= 0 && ctx->idx < MAX_CONNECTIONS) to_pump[ctx->idx] = 1;
            } else if (ctx->kind == EP_KIND_C2R_PW) {
                if (ctx->idx >= 0 && ctx->idx < MAX_CONNECTIONS) {
                    conns[ctx->idx].c2r_ingress_blocked = 0;
                    to_pump[ctx->idx] = 1;
                }
            } else if (ctx->kind == EP_KIND_R2C_PW) {
                if (ctx->idx >= 0 && ctx->idx < MAX_CONNECTIONS) {
                    conns[ctx->idx].r2c_ingress_blocked = 0;
                    to_pump[ctx->idx] = 1;
                }
            }
        }

        // Pump splice state machines for any connection that had an event
        const unsigned int sflags = SPLICE_F_NONBLOCK | SPLICE_F_MORE;
        for (int idx=0; idx<MAX_CONNECTIONS; idx++) {
            if (!to_pump[idx]) continue;
            conn_pair *c = &conns[idx];
            if (c->client_sock == 0 && c->remote_sock == 0) continue;

            int closed = 0;
            const char *close_reason = NULL;

            // bounded work per wakeup to avoid starvation
            for (int steps=0; steps<64 && !closed; steps++) {
                int progressed = 0;

                // egress: pipe -> sockets
                if (c->c2r_in_pipe > 0) {
                    size_t want = c->c2r_in_pipe;
                    if (want > (size_t)BUF_SIZE) want = (size_t)BUF_SIZE;
                    ssize_t m = splice(c->p_c2r[0], NULL, c->remote_sock, NULL, want, sflags);
                    if (m > 0) {
                        c->c2r_in_pipe -= (size_t)m;
                        c->bytes_c2r += (size_t)m;
                        clock_gettime(CLOCK_MONOTONIC, &c->last_activity_ts);
                        progressed = 1;
                        if (c->c2r_ingress_blocked) c->c2r_ingress_blocked = 0;
                    } else if (m < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
                        log_conn(c->conn_id, "splice pipe->remote failed");
                        closed = 1;
                        close_reason = "splice pipe->remote failed";
                    }
                }
                if (c->r2c_in_pipe > 0 && !closed) {
                    size_t want = c->r2c_in_pipe;
                    if (want > (size_t)BUF_SIZE) want = (size_t)BUF_SIZE;
                    ssize_t m = splice(c->p_r2c[0], NULL, c->client_sock, NULL, want, sflags);
                    if (m > 0) {
                        c->r2c_in_pipe -= (size_t)m;
                        c->bytes_r2c += (size_t)m;
                        clock_gettime(CLOCK_MONOTONIC, &c->last_activity_ts);
                        progressed = 1;
                        if (c->r2c_ingress_blocked) c->r2c_ingress_blocked = 0;
                    } else if (m < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
                        log_conn(c->conn_id, "splice pipe->client failed");
                        closed = 1;
                        close_reason = "splice pipe->client failed";
                    }
                }

                // ingress: sockets -> pipes
                if (!closed && c->client_read_open && !c->c2r_ingress_blocked) {
                    ssize_t m = splice(c->client_sock, NULL, c->p_c2r[1], NULL, (size_t)BUF_SIZE, sflags);
                    if (m > 0) {
                        c->c2r_in_pipe += (size_t)m;
                        clock_gettime(CLOCK_MONOTONIC, &c->last_activity_ts);
                        progressed = 1;
                    } else if (m == 0) {
                        log_conn(c->conn_id, "client disconnected");
                        c->client_read_open = 0;
                        c->pending_shutdown_remote_wr = 1;
                        if (!close_reason) close_reason = "client disconnect";
                        progressed = 1;
                    } else if (m < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            // Likely pipe full; enable an epoll watch on pipe write end to resume.
                            c->c2r_ingress_blocked = 1;
                        } else if (errno != EINTR) {
                            log_conn(c->conn_id, "splice client->pipe failed");
                            closed = 1;
                            close_reason = "splice client->pipe failed";
                        }
                    }
                }
                if (!closed && c->remote_read_open && !c->r2c_ingress_blocked) {
                    ssize_t m = splice(c->remote_sock, NULL, c->p_r2c[1], NULL, (size_t)BUF_SIZE, sflags);
                    if (m > 0) {
                        c->r2c_in_pipe += (size_t)m;
                        clock_gettime(CLOCK_MONOTONIC, &c->last_activity_ts);
                        progressed = 1;
                    } else if (m == 0) {
                        log_conn(c->conn_id, "remote disconnected");
                        c->remote_read_open = 0;
                        c->pending_shutdown_client_wr = 1;
                        if (!close_reason) close_reason = "remote disconnect";
                        progressed = 1;
                    } else if (m < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            c->r2c_ingress_blocked = 1;
                        } else if (errno != EINTR) {
                            log_conn(c->conn_id, "splice remote->pipe failed");
                            closed = 1;
                            close_reason = "splice remote->pipe failed";
                        }
                    }
                }

                // delayed half-close propagation: shutdown after the corresponding pipe drains
                if (!closed && c->pending_shutdown_remote_wr && c->c2r_in_pipe == 0) {
                    shutdown(c->remote_sock, SHUT_WR);
                    c->pending_shutdown_remote_wr = 0;
                    progressed = 1;
                }
                if (!closed && c->pending_shutdown_client_wr && c->r2c_in_pipe == 0) {
                    shutdown(c->client_sock, SHUT_WR);
                    c->pending_shutdown_client_wr = 0;
                    progressed = 1;
                }

                if (!progressed) break;
            }

            // idle timeout check
            if (!closed && idle_timeout_secs >= 0) {
                struct timespec now_ts;
                clock_gettime(CLOCK_MONOTONIC, &now_ts);
                long long idle_ms = elapsed_ms(&c->last_activity_ts, &now_ts);
                if (idle_ms > idle_timeout_secs * 1000LL) {
                    log_conn(c->conn_id, "idle timeout");
                    closed = 1;
                    close_reason = "idle timeout";
                }
            }

            // done when both sides EOF and pipes drained
            if (!closed && !c->client_read_open && !c->remote_read_open &&
                c->c2r_in_pipe == 0 && c->r2c_in_pipe == 0 &&
                !c->pending_shutdown_client_wr && !c->pending_shutdown_remote_wr) {
                closed = 1;
                if (!close_reason) close_reason = "eof";
            }

            // update epoll interest masks
            struct epoll_event evc;
            memset(&evc, 0, sizeof(evc));
            evc.events = conn_ep_events_client(c);
            evc.data.ptr = &conn_ctxs[idx*4];
            epoll_ctl(epfd, EPOLL_CTL_MOD, c->client_sock, &evc);
            struct epoll_event evr;
            memset(&evr, 0, sizeof(evr));
            evr.events = conn_ep_events_remote(c);
            evr.data.ptr = &conn_ctxs[idx*4+1];
            epoll_ctl(epfd, EPOLL_CTL_MOD, c->remote_sock, &evr);

            // Dynamically watch pipe write ends only while ingress is blocked.
            if (!closed && c->c2r_ingress_blocked && !c->watching_c2r_pw) {
                struct epoll_event evp;
                memset(&evp, 0, sizeof(evp));
                evp.events = EPOLLOUT | EPOLLERR | EPOLLHUP;
                evp.data.ptr = &conn_ctxs[idx*4+2];
                if (epoll_ctl(epfd, EPOLL_CTL_ADD, c->p_c2r[1], &evp) == 0) c->watching_c2r_pw = 1;
            }
            if (!closed && !c->c2r_ingress_blocked && c->watching_c2r_pw) {
                epoll_ctl(epfd, EPOLL_CTL_DEL, c->p_c2r[1], NULL);
                c->watching_c2r_pw = 0;
            }
            if (!closed && c->r2c_ingress_blocked && !c->watching_r2c_pw) {
                struct epoll_event evp;
                memset(&evp, 0, sizeof(evp));
                evp.events = EPOLLOUT | EPOLLERR | EPOLLHUP;
                evp.data.ptr = &conn_ctxs[idx*4+3];
                if (epoll_ctl(epfd, EPOLL_CTL_ADD, c->p_r2c[1], &evp) == 0) c->watching_r2c_pw = 1;
            }
            if (!closed && !c->r2c_ingress_blocked && c->watching_r2c_pw) {
                epoll_ctl(epfd, EPOLL_CTL_DEL, c->p_r2c[1], NULL);
                c->watching_r2c_pw = 0;
            }

            if (closed) {
                // close sockets and pipes (close removes from epoll automatically)
                close(c->client_sock);
                close(c->remote_sock);
                close(c->p_c2r[0]); close(c->p_c2r[1]);
                close(c->p_r2c[0]); close(c->p_r2c[1]);

                char hbuf[64], rbuf[64];
                human_bytes(c->bytes_c2r, hbuf, sizeof(hbuf));
                human_bytes(c->bytes_r2c, rbuf, sizeof(rbuf));
                struct timespec end_ts;
                clock_gettime(CLOCK_MONOTONIC, &end_ts);
                long long ms = elapsed_ms(&c->start_ts, &end_ts);
                log_conn(c->conn_id, "connection closed, uptime %.3fs, bytes: in %s, out %s%s%s",
                         (double)ms / 1000.0, hbuf, rbuf,
                         close_reason ? ", reason: " : "", close_reason ? close_reason : "");
                atomic_fetch_sub(&g_active_connections, 1);

                memset(c, 0, sizeof(*c));
                free_idxs[free_top++] = idx;
                nconns--;
            }
        }

        free(to_pump);
    }

    // Unreachable, but keep tidy for future changes.
    // free(fds);
    // free(conns);
    // for (int j=0;j<nlisten;j++) close(listen_socks[j]);
    // free(listen_socks);
    // free(specs);
    return 0;
}