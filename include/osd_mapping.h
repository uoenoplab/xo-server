#ifndef __OSD_MAPPING_H__
#define __OSD_MAPPING_H__

#define MAX_OSDS 64

extern char *osd_addr_strs[MAX_OSDS];
extern struct sockaddr_in osd_addrs[MAX_OSDS];
extern int osd_ids[MAX_OSDS];
extern int num_osds;
extern int num_peers;

static inline char *get_my_osd_addr_str()
{
    return osd_addr_strs[num_osds - 1];
}

static inline int get_my_osd_id()
{
    return osd_ids[num_osds - 1];
}

#endif
