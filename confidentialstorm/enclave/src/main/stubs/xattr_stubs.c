#include <errno.h>
#include <sys/types.h>

ssize_t fgetxattr(int fd, const char *name, void *value, size_t size) { errno = ENOTSUP; return -1; }
int     fsetxattr(int fd, const char *name, const void *value, size_t size, int flags) { errno = ENOTSUP; return -1; }
int     fremovexattr(int fd, const char *name) { errno = ENOTSUP; return -1; }
ssize_t flistxattr(int fd, char *list, size_t size) { errno = ENOTSUP; return -1; }