/*
  FUSE: Filesystem in Userspace
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

  This program can be distributed under the terms of the GNU GPLv2.
  See the file GPL2.txt.
*/

/** @file
 *
 * Configurable stubbed filesystem using low-level API
 *
 * Environment variables:
 * - C_STUB_NUMFILES: Number of files (default: 10)
 * - C_STUB_BACKGROUND_THREADS: Background threads (default: 64)
 * - C_STUB_READSIZE: Read buffer size (default: 4096)
 * - STUB_DISTR: Latency distribution ("normal" or unset for no latency)
 * - STUB_DISTR_MEAN: Mean latency in microseconds (default: 1000)
 * - STUB_DISTR_STDDEV: Standard deviation in microseconds (default: 100)
 *
 * Compile with:
 *     gcc -Wall stub.c `pkg-config fuse3 --cflags --libs` -lm -o stub
 */

#define FUSE_USE_VERSION FUSE_MAKE_VERSION(3, 12)

#include <fuse_lowlevel.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <math.h>
#include <time.h>
#include <sys/time.h>

/* Default values */
#define DEFAULT_NUM_FILES 10
#define DEFAULT_BACKGROUND_THREADS 64
#define DEFAULT_READSIZE 4096
#define DEFAULT_LATENCY_MEAN 1000    /* microseconds */
#define DEFAULT_LATENCY_STDDEV 100   /* microseconds */

/* File size: 100GiB as specified in requirements */
#define FILE_SIZE_GB 100
#define FILE_SIZE (FILE_SIZE_GB * 1024LL * 1024LL * 1024LL)

/* Global configuration variables - initialize with defaults */
static int g_num_files = DEFAULT_NUM_FILES;
static int g_background_threads = DEFAULT_BACKGROUND_THREADS;
static size_t g_readsize = DEFAULT_READSIZE;
static int g_use_latency = 0;
static double g_latency_mean = DEFAULT_LATENCY_MEAN;
static double g_latency_stddev = DEFAULT_LATENCY_STDDEV;
static int g_initialized = 0;

/* Pre-allocated buffer for efficient reading */
static char *g_data_buffer = NULL;

/* Random number generator state for latency simulation */
static unsigned int g_rand_seed;

/* Initialize configuration from environment variables */
static void init_config(void)
{
    char *env_val;

    if (g_initialized) return;  /* Already initialized */

    /* Number of files */
    env_val = getenv("C_STUB_NUMFILES");
    if (env_val) {
        int val = atoi(env_val);
        if (val > 0 && val <= 10000) {  /* Reasonable limits */
            g_num_files = val;
        }
    }

    /* Background threads */
    env_val = getenv("C_STUB_BACKGROUND_THREADS");
    if (env_val) {
        int val = atoi(env_val);
        if (val > 0 && val <= 1000) {  /* Reasonable limits */
            g_background_threads = val;
        }
    }

    /* Read buffer size */
    env_val = getenv("C_STUB_READSIZE");
    if (env_val) {
        long val = atol(env_val);
        if (val > 0 && val <= 100*1024*1024) {  /* Max 100MB buffer */
            g_readsize = (size_t)val;
        }
    }

    /* Latency distribution */
    env_val = getenv("STUB_DISTR");
    if (env_val && strcmp(env_val, "normal") == 0) {
        g_use_latency = 1;

        env_val = getenv("STUB_DISTR_MEAN");
        if (env_val) {
            double val = atof(env_val);
            if (val >= 0 && val <= 1000000) {  /* Max 1 second */
                g_latency_mean = val;
            }
        }

        env_val = getenv("STUB_DISTR_STDDEV");
        if (env_val) {
            double val = atof(env_val);
            if (val >= 0 && val <= 1000000) {  /* Max 1 second */
                g_latency_stddev = val;
            }
        }
    }

    g_initialized = 1;

    printf("Configuration:\n");
    printf("  Files: %d (each %d GB)\n", g_num_files, FILE_SIZE_GB);
    printf("  Background threads: %d\n", g_background_threads);
    printf("  Read buffer size: %zu bytes\n", g_readsize);
    if (g_use_latency) {
        printf("  Latency simulation: normal distribution (mean=%.1f µs, stddev=%.1f µs)\n",
               g_latency_mean, g_latency_stddev);
    } else {
        printf("  Latency simulation: disabled\n");
    }
}

/* Generate normally distributed random number using Box-Muller transform */
static double normal_random(double mean, double stddev)
{
    static int has_spare = 0;
    static double spare;

    if (has_spare) {
        has_spare = 0;
        return spare * stddev + mean;
    }

    has_spare = 1;
    double u = ((double)rand_r(&g_rand_seed)) / RAND_MAX;
    double v = ((double)rand_r(&g_rand_seed)) / RAND_MAX;

    /* Avoid log(0) */
    if (u == 0.0) u = 1e-10;

    double mag = stddev * sqrt(-2.0 * log(u));
    spare = mag * cos(2.0 * M_PI * v);
    return mag * sin(2.0 * M_PI * v) + mean;
}

/* Simulate latency if configured */
static void simulate_latency(void)
{
    if (!g_use_latency) return;

    double latency_us = normal_random(g_latency_mean, g_latency_stddev);
    if (latency_us > 0) {
        usleep((useconds_t)latency_us);
    }
}

/* Helper function to check if an inode is a valid file inode */
static int is_stub_file_ino(fuse_ino_t ino)
{
    return ino >= 2 && ino <= (1 + g_num_files);
}

/* Convert inode to file index (0-based) */
static int ino_to_file_index(fuse_ino_t ino)
{
    return (int)(ino - 2);
}

/* Convert file index (0-based) to inode */
static fuse_ino_t file_index_to_ino(int index)
{
    return (fuse_ino_t)(index + 2);
}

static int stub_stat(fuse_ino_t ino, struct stat *stbuf)
{
    if (!stbuf) return -1;

    memset(stbuf, 0, sizeof(*stbuf));
    stbuf->st_ino = ino;

    switch (ino) {
    case 1:
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
        break;

    default:
        if (is_stub_file_ino(ino)) {
            stbuf->st_mode = S_IFREG | 0444;
            stbuf->st_nlink = 1;
            stbuf->st_size = FILE_SIZE;
        } else {
            return -1;
        }
        break;
    }
    return 0;
}

static void stub_ll_init(void *userdata, struct fuse_conn_info *conn)
{
    (void)userdata;

    printf("FUSE init called\n");

    /* Initialize configuration first */
    init_config();

    /* Initialize random seed for latency simulation */
    struct timeval tv;
    gettimeofday(&tv, NULL);
    g_rand_seed = tv.tv_sec ^ tv.tv_usec ^ getpid();

    /* Initialize data buffer with configurable size */
    if (g_data_buffer) {
        free(g_data_buffer);  /* Clean up any previous allocation */
    }

    g_data_buffer = malloc(g_readsize);
    if (g_data_buffer == NULL) {
        fprintf(stderr, "Failed to allocate data buffer of size %zu\n", g_readsize);
        /* Don't exit here, let FUSE handle the error */
        return;
    }
    memset(g_data_buffer, '0', g_readsize);

    /* Configure FUSE connection */
    if (conn) {
        conn->want = 0;  /* Start with no capabilities */
        /* Set max background threads from configuration */
        conn->max_background = g_background_threads;
    }

    printf("FUSE filesystem initialized successfully\n");
}

static void stub_ll_destroy(void *userdata)
{
    (void)userdata;

    printf("FUSE destroy called\n");

    if (g_data_buffer) {
        free(g_data_buffer);
        g_data_buffer = NULL;
    }
}

static void stub_ll_getattr(fuse_req_t req, fuse_ino_t ino,
                             struct fuse_file_info *fi)
{
    struct stat stbuf;

    (void) fi;

    if (!req) {
        return;
    }

    simulate_latency();

    if (stub_stat(ino, &stbuf) == -1)
        fuse_reply_err(req, ENOENT);
    else
        fuse_reply_attr(req, &stbuf, 1.0);
}

static void stub_ll_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
    struct fuse_entry_param e;

    if (!req || !name) {
        if (req) fuse_reply_err(req, EINVAL);
        return;
    }

    simulate_latency();

    if (parent != 1) {
        fuse_reply_err(req, ENOENT);
        return;
    }

    /* Check if the name matches j{num}_100GiB.bin pattern */
    int file_index;
    if (sscanf(name, "j%d_100GiB.bin", &file_index) == 1 &&
        file_index >= 0 && file_index < g_num_files) {
        memset(&e, 0, sizeof(e));
        e.ino = file_index_to_ino(file_index);
        e.attr_timeout = 1.0;
        e.entry_timeout = 1.0;
        if (stub_stat(e.ino, &e.attr) == 0) {
            fuse_reply_entry(req, &e);
        } else {
            fuse_reply_err(req, ENOENT);
        }
    } else {
        fuse_reply_err(req, ENOENT);
    }
}

struct dirbuf {
    char *p;
    size_t size;
};

static void dirbuf_add(fuse_req_t req, struct dirbuf *b, const char *name,
                       fuse_ino_t ino)
{
    struct stat stbuf;
    size_t oldsize = b->size;

    if (!req || !b || !name) return;

    b->size += fuse_add_direntry(req, NULL, 0, name, NULL, 0);
    char *new_p = (char *) realloc(b->p, b->size);
    if (!new_p) {
        /* Realloc failed, keep old buffer */
        b->size = oldsize;
        return;
    }
    b->p = new_p;

    memset(&stbuf, 0, sizeof(stbuf));
    stbuf.st_ino = ino;
    fuse_add_direntry(req, b->p + oldsize, b->size - oldsize, name, &stbuf,
                      b->size);
}

#define min(x, y) ((x) < (y) ? (x) : (y))

static int reply_buf_limited(fuse_req_t req, const char *buf, size_t bufsize,
                             off_t off, size_t maxsize)
{
    if (!req) return -1;

    if (off < bufsize)
        return fuse_reply_buf(req, buf + off,
                              min(bufsize - off, maxsize));
    else
        return fuse_reply_buf(req, NULL, 0);
}

static void stub_ll_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
                             off_t off, struct fuse_file_info *fi)
{
    (void) fi;

    if (!req) return;

    simulate_latency();

    if (ino != 1) {
        fuse_reply_err(req, ENOTDIR);
    } else {
        struct dirbuf b;
        char filename[64];
        int i;

        memset(&b, 0, sizeof(b));
        dirbuf_add(req, &b, ".", 1);
        dirbuf_add(req, &b, "..", 1);

        /* Add all j{num}_100GiB.bin files based on configuration */
        for (i = 0; i < g_num_files; i++) {
            snprintf(filename, sizeof(filename), "j%d_100GiB.bin", i);
            dirbuf_add(req, &b, filename, file_index_to_ino(i));
        }

        reply_buf_limited(req, b.p, b.size, off, size);
        if (b.p) free(b.p);
    }
}

static void stub_ll_open(fuse_req_t req, fuse_ino_t ino,
                          struct fuse_file_info *fi)
{
    if (!req) return;

    simulate_latency();

    if (!is_stub_file_ino(ino))
        fuse_reply_err(req, EISDIR);
    else if ((fi->flags & O_ACCMODE) != O_RDONLY)
        fuse_reply_err(req, EACCES);
    else
        fuse_reply_open(req, fi);
}

static void stub_ll_read(fuse_req_t req, fuse_ino_t ino, size_t size,
                          off_t off, struct fuse_file_info *fi)
{
    (void) fi;

    if (!req) return;

    if (!is_stub_file_ino(ino)) {
        fuse_reply_err(req, EINVAL);
        return;
    }

    simulate_latency();

    /* Check if offset is beyond file size */
    if (off >= FILE_SIZE) {
        fuse_reply_buf(req, NULL, 0);
        return;
    }

    /* Calculate how much data to return */
    size_t remaining = FILE_SIZE - off;
    size_t to_read = min(size, remaining);

    /* Return data */
    if (to_read > 0) {
        if (to_read <= g_readsize && g_data_buffer) {
            /* Use pre-allocated buffer for reads within configured size */
            fuse_reply_buf(req, g_data_buffer, to_read);
        } else {
            /* Allocate buffer for large reads */
            char *read_buffer = malloc(to_read);
            if (read_buffer == NULL) {
                fuse_reply_err(req, ENOMEM);
                return;
            }
            memset(read_buffer, '0', to_read);
            fuse_reply_buf(req, read_buffer, to_read);
            free(read_buffer);
        }
    } else {
        fuse_reply_buf(req, NULL, 0);
    }
}

static const struct fuse_lowlevel_ops stub_ll_oper = {
    .init = stub_ll_init,
    .destroy = stub_ll_destroy,
    .lookup = stub_ll_lookup,
    .getattr = stub_ll_getattr,
    .readdir = stub_ll_readdir,
    .open = stub_ll_open,
    .read = stub_ll_read,
};

int main(int argc, char *argv[])
{
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
    struct fuse_session *se = NULL;
    struct fuse_cmdline_opts opts;
    int ret = -1;

    /* Initialize config early */
    init_config();

    if (fuse_parse_cmdline(&args, &opts) != 0) {
        fprintf(stderr, "Failed to parse command line\n");
        return 1;
    }

    if (opts.show_help) {
        printf("usage: %s [options] <mountpoint>\n\n", argv[0]);
        printf("Environment variables:\n");
        printf("  C_STUB_NUMFILES          Number of files (default: %d)\n", DEFAULT_NUM_FILES);
        printf("  C_STUB_BACKGROUND_THREADS Background threads (default: %d)\n", DEFAULT_BACKGROUND_THREADS);
        printf("  C_STUB_READSIZE          Read buffer size (default: %d)\n", DEFAULT_READSIZE);
        printf("  STUB_DISTR               Latency distribution ('normal' or unset)\n");
        printf("  STUB_DISTR_MEAN          Mean latency in µs (default: %d)\n", DEFAULT_LATENCY_MEAN);
        printf("  STUB_DISTR_STDDEV        Stddev latency in µs (default: %d)\n", DEFAULT_LATENCY_STDDEV);
        printf("\n");
        printf("Files will be named: j1_100GiB.bin, j2_100GiB.bin, ..., j{N}_100GiB.bin\n");
        printf("\n");
        fuse_cmdline_help();
        fuse_lowlevel_help();
        ret = 0;
        goto cleanup;
    } else if (opts.show_version) {
        printf("FUSE library version %s\n", fuse_pkgversion());
        fuse_lowlevel_version();
        ret = 0;
        goto cleanup;
    }

    if(opts.mountpoint == NULL) {
        printf("usage: %s [options] <mountpoint>\n", argv[0]);
        printf("       %s --help\n", argv[0]);
        ret = 1;
        goto cleanup;
    }

    printf("Creating FUSE session...\n");
    se = fuse_session_new(&args, &stub_ll_oper,
                          sizeof(stub_ll_oper), NULL);
    if (se == NULL) {
        fprintf(stderr, "Failed to create FUSE session\n");
        goto cleanup;
    }

    printf("Setting signal handlers...\n");
    if (fuse_set_signal_handlers(se) != 0) {
        fprintf(stderr, "Failed to set signal handlers\n");
        goto cleanup;
    }

    printf("Mounting filesystem at %s...\n", opts.mountpoint);
    if (fuse_session_mount(se, opts.mountpoint) != 0) {
        fprintf(stderr, "Failed to mount filesystem\n");
        goto cleanup_signals;
    }

    if (!opts.foreground) {
        printf("Daemonizing...\n");
        fuse_daemonize(opts.foreground);
    }

    printf("Starting FUSE main loop...\n");
    /* Block until ctrl+c or fusermount -u */
    if (opts.singlethread)
        ret = fuse_session_loop(se);
    else
        ret = fuse_session_loop_mt(se, 0);

    printf("FUSE loop ended with code %d\n", ret);
    fuse_session_unmount(se);

cleanup_signals:
    fuse_remove_signal_handlers(se);
cleanup:
    if (se) {
        fuse_session_destroy(se);
    }
    if (opts.mountpoint) {
        free(opts.mountpoint);
    }
    fuse_opt_free_args(&args);

    /* Cleanup global resources */
    if (g_data_buffer) {
        free(g_data_buffer);
        g_data_buffer = NULL;
    }

    printf("Exiting with code %d\n", ret ? 1 : 0);
    return ret ? 1 : 0;
}
