#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <inttypes.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

static const size_t kMaxLineBytes = 32 * 1024; // 32KiB

static void usage(FILE *out) {
  fprintf(out,
          "Usage: logd --out FILE [--keep N] [--size BYTES[K|M|G|T]] "
          "[--no-compress]\n"
          "\n"
          "Options:\n"
          "  --out FILE         Output log file path (required)\n"
          "  --keep N           Keep N rotated files (default: 10)\n"
          "  --size S           Rotate when file size after write >= S "
          "(default: 10M)\n"
          "                     S is bytes by default; optional suffix K/M/G/T means KiB/MiB/GiB/TiB\n"
          "                     Examples: 1048576, 44K, 10M, 1G\n"
          "  --no-compress       Disable gzip compression on rotation (default: compress)\n"
          "  --help             Show this help\n");
}

static bool parse_u64(const char *s, uint64_t *out) {
  if (!s || !*s) return false;
  errno = 0;
  char *end = NULL;
  unsigned long long v = strtoull(s, &end, 10);
  if (errno != 0 || end == s || *end != '\0') return false;
  *out = (uint64_t)v;
  return true;
}

static bool parse_size_bytes(const char *s, uint64_t *out_bytes) {
  if (!s || !*s) return false;
  errno = 0;
  char *end = NULL;
  unsigned long long base = strtoull(s, &end, 10);
  if (errno != 0 || end == s) return false;

  uint64_t mult = 1;
  if (*end != '\0') {
    char suf = *end;
    if (end[1] != '\0') return false;
    switch (suf) {
      case 'K':
      case 'k':
        mult = 1024ULL;
        break;
      case 'M':
      case 'm':
        mult = 1024ULL * 1024ULL;
        break;
      case 'G':
      case 'g':
        mult = 1024ULL * 1024ULL * 1024ULL;
        break;
      case 'T':
      case 't':
        mult = 1024ULL * 1024ULL * 1024ULL * 1024ULL;
        break;
      default:
        return false;
    }
  }

  __uint128_t prod = (__uint128_t)base * (__uint128_t)mult;
  if (prod > UINT64_MAX) return false;
  *out_bytes = (uint64_t)prod;
  return true;
}

static bool file_exists(const char *path) {
  struct stat st;
  return stat(path, &st) == 0;
}

static void best_effort_unlink(const char *path) {
  if (!path) return;
  if (unlink(path) == 0) return;
  if (errno == ENOENT) return;
  // best effort: ignore other failures
}

static int run_gzip_best_effort(const char *path) {
  // gzip -f PATH
  pid_t pid = fork();
  if (pid < 0) return -1;
  if (pid == 0) {
    execlp("gzip", "gzip", "-f", path, (char *)NULL);
    _exit(127);
  }
  int status = 0;
  if (waitpid(pid, &status, 0) < 0) return -1;
  if (WIFEXITED(status) && WEXITSTATUS(status) == 0) return 0;
  return -1;
}

static bool get_file_size_bytes(FILE *f, uint64_t *out) {
  int fd = fileno(f);
  if (fd < 0) return false;
  struct stat st;
  if (fstat(fd, &st) != 0) return false;
  *out = (uint64_t)st.st_size;
  return true;
}

static char *mk_rot_name(const char *base, int idx, bool gz) {
  // base.idx[.gz]
  size_t need = strlen(base) + 1 /* . */ + 20 /* idx */ + (gz ? 3 : 0) + 1;
  char *p = (char *)malloc(need);
  if (!p) return NULL;
  if (gz) {
    snprintf(p, need, "%s.%d.gz", base, idx);
  } else {
    snprintf(p, need, "%s.%d", base, idx);
  }
  return p;
}

static int rotate_logs(const char *out_path, uint64_t keep_n, bool compress) {
  // Delete oldest (both .N and .N.gz), shift .i -> .i+1 (preferring .gz), then move base -> .1,
  // then gzip .1 if enabled.
  if (keep_n == 0) {
    // Keep no rotated files: discard the rotated content.
    best_effort_unlink(out_path);
    return 0;
  }

  char *oldest = mk_rot_name(out_path, (int)keep_n, false);
  char *oldest_gz = mk_rot_name(out_path, (int)keep_n, true);
  if (!oldest || !oldest_gz) {
    free(oldest);
    free(oldest_gz);
    return -1;
  }
  best_effort_unlink(oldest);
  best_effort_unlink(oldest_gz);
  free(oldest);
  free(oldest_gz);

  for (int i = (int)keep_n - 1; i >= 1; i--) {
    char *src_gz = mk_rot_name(out_path, i, true);
    char *dst_gz = mk_rot_name(out_path, i + 1, true);
    char *src = mk_rot_name(out_path, i, false);
    char *dst = mk_rot_name(out_path, i + 1, false);
    if (!src_gz || !dst_gz || !src || !dst) {
      free(src_gz);
      free(dst_gz);
      free(src);
      free(dst);
      return -1;
    }

    if (file_exists(src_gz)) {
      (void)rename(src_gz, dst_gz);
    } else if (file_exists(src)) {
      (void)rename(src, dst);
    }

    free(src_gz);
    free(dst_gz);
    free(src);
    free(dst);
  }

  char *n1 = mk_rot_name(out_path, 1, false);
  char *n1gz = mk_rot_name(out_path, 1, true);
  if (!n1 || !n1gz) {
    free(n1);
    free(n1gz);
    return -1;
  }
  best_effort_unlink(n1);
  best_effort_unlink(n1gz);
  if (rename(out_path, n1) != 0) {
    free(n1);
    free(n1gz);
    return -1;
  }
  if (compress) (void)run_gzip_best_effort(n1);
  free(n1);
  free(n1gz);
  return 0;
}

static FILE *open_log_append(const char *path) {
  FILE *f = fopen(path, "a");
  return f;
}

static bool read_line_capped(FILE *in, char *buf, size_t buf_sz, size_t *out_len) {
  // Reads one line (including '\n' if present). Enforces maximum line length of (buf_sz-1).
  // Returns false on EOF with no bytes read; true otherwise (even if last line has no '\n').
  if (!fgets(buf, (int)buf_sz, in)) return false;
  size_t len = strlen(buf);

  if (len == 0) return true;

  if (buf[len - 1] == '\n') {
    *out_len = len;
    return true;
  }

  // No newline in buffer: if we hit EOF it's a final line without newline; otherwise it's too long.
  if (feof(in)) {
    *out_len = len;
    return true;
  }

  // Drain remainder of the long line.
  int c;
  while ((c = fgetc(in)) != EOF) {
    if (c == '\n') break;
  }
  errno = EMSGSIZE;
  return false;
}

int main(int argc, char **argv) {
  const char *out_path = NULL;
  uint64_t keep_n = 10;
  bool compress = true;
  uint64_t rotate_size = 10ULL * 1024ULL * 1024ULL; // 10M default (MiB)

  static struct option longopts[] = {
      {"out", required_argument, NULL, 'o'},
      {"keep", required_argument, NULL, 'k'},
      {"no-compress", no_argument, NULL, 'C'},
      {"size", required_argument, NULL, 's'},
      {"help", no_argument, NULL, 'h'},
      {0, 0, 0, 0},
  };

  int ch;
  while ((ch = getopt_long(argc, argv, "o:k:Cs:h", longopts, NULL)) != -1) {
    switch (ch) {
      case 'o':
        out_path = optarg;
        break;
      case 'k': {
        uint64_t v = 0;
        if (!parse_u64(optarg, &v)) {
          fprintf(stderr, "Invalid --keep: %s\n", optarg);
          return 2;
        }
        if (v > (uint64_t)INT_MAX) {
          fprintf(stderr, "--keep is too large\n");
          return 2;
        }
        keep_n = v;
        break;
      }
      case 'C':
        compress = false;
        break;
      case 's': {
        uint64_t b = 0;
        if (!parse_size_bytes(optarg, &b) || b == 0) {
          fprintf(stderr, "Invalid --size: %s\n", optarg);
          return 2;
        }
        rotate_size = b;
        break;
      }
      case 'h':
        usage(stdout);
        return 0;
      default:
        usage(stderr);
        return 2;
    }
  }

  if (!out_path || !*out_path) {
    fprintf(stderr, "--out is required\n");
    usage(stderr);
    return 2;
  }

  FILE *out = open_log_append(out_path);
  if (!out) {
    fprintf(stderr, "Failed to open --out %s: %s\n", out_path, strerror(errno));
    return 1;
  }

  // If the existing file is already at/over the threshold, rotate immediately so new input
  // starts in a fresh file.
  uint64_t initial_sz = 0;
  if (get_file_size_bytes(out, &initial_sz) && initial_sz >= rotate_size) {
    if (fclose(out) != 0) {
      fprintf(stderr, "Close failed: %s\n", strerror(errno));
      return 1;
    }
    if (rotate_logs(out_path, keep_n, compress) != 0) {
      fprintf(stderr, "Rotation failed: %s\n", strerror(errno));
      return 1;
    }
    out = fopen(out_path, "w");
    if (!out) {
      fprintf(stderr, "Failed to open %s: %s\n", out_path, strerror(errno));
      return 1;
    }
  }

  char *line = (char *)malloc(kMaxLineBytes + 2); // + '\n' + '\0'
  if (!line) {
    fprintf(stderr, "Out of memory\n");
    fclose(out);
    return 1;
  }

  while (1) {
    size_t len = 0;
    if (!read_line_capped(stdin, line, kMaxLineBytes + 2, &len)) {
      if (feof(stdin)) break;
      fprintf(stderr, "Input line exceeds 32KiB\n");
      free(line);
      fclose(out);
      return 1;
    }

    if (len == 0) continue;

    size_t wrote = fwrite(line, 1, len, out);
    if (wrote != len) {
      fprintf(stderr, "Write failed: %s\n", strerror(errno));
      free(line);
      fclose(out);
      return 1;
    }
    if (fflush(out) != 0) {
      fprintf(stderr, "Flush failed: %s\n", strerror(errno));
      free(line);
      fclose(out);
      return 1;
    }

    uint64_t sz = 0;
    if (get_file_size_bytes(out, &sz) && sz >= rotate_size) {
      if (fclose(out) != 0) {
        fprintf(stderr, "Close failed: %s\n", strerror(errno));
        free(line);
        return 1;
      }
      if (rotate_logs(out_path, keep_n, compress) != 0) {
        fprintf(stderr, "Rotation failed: %s\n", strerror(errno));
        free(line);
        return 1;
      }
      out = fopen(out_path, "w"); // new file after rotation
      if (!out) {
        fprintf(stderr, "Failed to reopen %s: %s\n", out_path, strerror(errno));
        free(line);
        return 1;
      }
    }
  }

  free(line);
  (void)fclose(out);
  return 0;
}

