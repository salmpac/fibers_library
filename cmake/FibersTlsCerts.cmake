# fibers_tls_certs(<target> <dir>): a target that generates the throwaway
# test PKI (tests/tls/gen_certs.sh) into <dir> with the openssl CLI at build
# time. Used by the TLS tests, benchmark and example; nothing is committed.
function(fibers_tls_certs target dir)
    add_custom_command(OUTPUT ${dir}/stamp
        COMMAND sh ${PROJECT_SOURCE_DIR}/tests/tls/gen_certs.sh ${dir} ${FIBERS_OPENSSL_CLI}
        DEPENDS ${PROJECT_SOURCE_DIR}/tests/tls/gen_certs.sh
        COMMENT "Generating TLS test certificates in ${dir}"
        VERBATIM)
    add_custom_target(${target} DEPENDS ${dir}/stamp)
endfunction()
