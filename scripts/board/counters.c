// counters CMD...: runs CMD, counting user-space cycles, instructions, cache-misses, LLC loads and LLC
// load misses over it and its children, and prints them to stderr as one line.
#define _GNU_SOURCE
#include <linux/perf_event.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
static int open_ev(uint32_t type, uint64_t config, pid_t pid) {
  struct perf_event_attr a; memset(&a, 0, sizeof a);
  a.size = sizeof a; a.type = type; a.config = config;
  a.disabled = 1; a.enable_on_exec = 1; a.inherit = 1;
  a.exclude_kernel = 1; a.exclude_hv = 1;
  return syscall(SYS_perf_event_open, &a, pid, -1, -1, 0);
}
int main(int argc, char **argv) {
  if (argc < 2) { fprintf(stderr, "usage: counters CMD...\n"); return 2; }
  int go[2]; if (pipe(go)) return 3;
  pid_t child = fork();
  if (child == 0) { char c; close(go[1]); if (read(go[0], &c, 1) != 1) _exit(4); execvp(argv[1], argv + 1); _exit(127); }
  close(go[0]);
  const char *names[] = {"cycles", "instructions", "cache_misses", "llc_loads", "llc_misses"};
  uint64_t ll = PERF_COUNT_HW_CACHE_LL;
  struct { uint32_t t; uint64_t c; } ev[] = {
    {PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES},
    {PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS},
    {PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_MISSES},
    {PERF_TYPE_HW_CACHE, ll | (PERF_COUNT_HW_CACHE_OP_READ << 8) | (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16)},
    {PERF_TYPE_HW_CACHE, ll | (PERF_COUNT_HW_CACHE_OP_READ << 8) | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16)},
  };
  int fd[5];
  for (int i = 0; i < 5; i++) { fd[i] = open_ev(ev[i].t, ev[i].c, child); if (fd[i] < 0) perror(names[i]); }
  if (write(go[1], "x", 1) != 1) return 5;
  int st; waitpid(child, &st, 0);
  fprintf(stderr, "counters");
  for (int i = 0; i < 5; i++) { uint64_t v = 0; if (fd[i] >= 0 && read(fd[i], &v, 8) == 8) fprintf(stderr, " %s %llu", names[i], (unsigned long long)v); else fprintf(stderr, " %s na", names[i]); }
  fprintf(stderr, "\n");
  return WIFEXITED(st) ? WEXITSTATUS(st) : 1;
}
