#include <openssl/evp.h>
#include <openssl/kdf.h>
#include <openssl/core_names.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int derive_hkdf_key(const unsigned char *secret, size_t secret_len, const char *info, unsigned char *out_key, size_t out_key_len) {
    EVP_KDF *kdf = NULL;
    EVP_KDF_CTX *kctx = NULL;
    OSSL_PARAM params[8], *p = params;
    int ret = 0;

    kdf = EVP_KDF_fetch(NULL, "TLS13-KDF", NULL);
    if (kdf == NULL) return 0;

    kctx = EVP_KDF_CTX_new(kdf);
    if (kctx == NULL) goto end;

    *p++ = OSSL_PARAM_construct_utf8_string(OSSL_KDF_PARAM_DIGEST, SN_sha384, strlen(SN_sha384));

    *p++ = OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_KEY, (void*)secret, secret_len);
    *p++ = OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_SALT, "", 0);

    *p++ = OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_PREFIX, "tls13 ", strlen("tls13 "));
    *p++ = OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_LABEL, "key", strlen("key"));

    *p++ = OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_DATA, "", 0);

    *p++ = OSSL_PARAM_construct_utf8_string(OSSL_KDF_PARAM_MODE, "EXPAND_ONLY", strlen("EXPAND_ONLY"));
    *p = OSSL_PARAM_construct_end();

    // 执行KDF操作
    if (EVP_KDF_derive(kctx, out_key, out_key_len, params) <= 0) {
        ret = 0;
        goto end;
    }

    ret = 1;

end:
    EVP_KDF_CTX_free(kctx);
    EVP_KDF_free(kdf);

    return ret;
}

static void hexdump(const char *title, void *buf, size_t len) {
  printf("%s (%lu bytes) :\n", title, len);
  for (int i = 0; i < len; i++) {
    printf("%02hhX ", ((uint8_t *)buf)[i]);
    if (i % 16 == 15) printf("\n");
  }
  printf("\n");
}



int main() {
    unsigned char hex_secret[] = "f08b40419f01eb0dbc4d8989e01833eb8a61e75adb3044050d0e252bfcd60a84e4d3d4df384f183cd0f62f16ea9480a7";
    size_t hex_secret_len = strlen(hex_secret);
    unsigned char secret[hex_secret_len / 2]; // 由于每两个十六进制字符转换为一个字节
    hex_to_bytes(hex_secret, secret, sizeof(secret));
    unsigned char out_key[32];

    hexdump("traffic secret", secret, sizeof(secret));

    struct timespec start, end;
    long elapsed_microseconds;
    clock_gettime(CLOCK_MONOTONIC_RAW, &start);
    if (derive_hkdf_key(secret, sizeof(secret), "key", out_key, sizeof(out_key))) {
        clock_gettime(CLOCK_MONOTONIC_RAW, &end);
        elapsed_microseconds = (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_nsec - start.tv_nsec) / 1000;
        printf("Key Derived Successfully.\n");
        printf("Function execution time: %ld microseconds\n", elapsed_microseconds);
        hexdump("", out_key, 32);
    } else {
        printf("Error Deriving Key.\n");
    }

    return 0;
}
