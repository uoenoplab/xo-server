#ifndef __UTIL_H__
#define __UTIL_H__

double elapsed_time(struct timespec a, struct timespec b);
void get_datetime_str(char *buf, size_t length);
void convertToISODateTime(const char* inputDateTime, char* outputISODateTime);
void unescapeHtml(char* str);

#endif
