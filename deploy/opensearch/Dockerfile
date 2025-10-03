FROM opensearchproject/opensearch:3.8.0

RUN /usr/share/opensearch/bin/opensearch-plugin install --batch repository-s3

COPY --chown=opensearch:opensearch entrypoint.sh /usr/local/bin/dataplane-entrypoint
ENTRYPOINT ["/usr/local/bin/dataplane-entrypoint"]
