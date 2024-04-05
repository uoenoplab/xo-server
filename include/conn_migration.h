#ifndef __CONN_MIGRATION_H__
#define __CONN_MIGRATION_H__

#include "http_client.h"

void conn_migration(struct http_client *client, int target_osd_id);

#endif
