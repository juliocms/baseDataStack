FROM public.ecr.aws/amazonlinux/amazonlinux:2023-minimal

# TARGETARCH vem do BuildKit: amd64 ou arm64.
ARG TARGETARCH=amd64

RUN dnf install -y curl-minimal \
    && dnf clean all \
    && case "$TARGETARCH" in \
         amd64) arch=x86_64 ;; \
         arm64) arch=aarch64 ;; \
         *) echo "arquitetura nao suportada: $TARGETARCH" && exit 1 ;; \
       esac \
    && curl -fsSL -o /tmp/aws-emulator.rpm \
         "https://downloads.aws.com/aws-emulator/packages/aws-emulator/LTS/latest/aws-emulator-${arch}.rpm" \
    && rpm -i /tmp/aws-emulator.rpm \
    && rm /tmp/aws-emulator.rpm \
    && mkdir -p /home/emulator

COPY entrypoint.sh /entrypoint.sh
RUN chmod 755 /entrypoint.sh

ENV HOME=/home/emulator \
    EMULATOR_BIND_ADDRESS=0.0.0.0 \
    EMULATOR_PORT=9297

EXPOSE 9297
ENTRYPOINT ["/entrypoint.sh"]
