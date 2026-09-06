FROM debian:trixie-slim AS wine-runtime

ARG WINE_STAGING_VERSION=11.16~trixie-1

ENV DEBIAN_FRONTEND=noninteractive \
    PATH=/opt/wine-staging/bin:$PATH \
    WINEDLLOVERRIDES=mscoree,mshtml= \
    WINEARCH=win64 \
    WINEDEBUG=-all

RUN dpkg --add-architecture i386 \
    && apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl \
    && install -d -m 0755 /etc/apt/keyrings /etc/apt/sources.list.d \
    && curl --fail --silent --show-error --location \
      https://dl.winehq.org/wine-builds/winehq.key \
      --output /etc/apt/keyrings/winehq-archive.key \
    && curl --fail --silent --show-error --location \
      https://dl.winehq.org/wine-builds/debian/dists/trixie/winehq-trixie.sources \
      --output /etc/apt/sources.list.d/winehq-trixie.sources \
    && apt-get update \
    && apt-get install --yes --no-install-recommends \
      "wine-staging=$WINE_STAGING_VERSION" \
    && rm -rf /var/lib/apt/lists/*

FROM wine-runtime AS prefix-builder

ENV DISPLAY=:99 \
    WINEPREFIX=/opt/jvlink/pristine-prefix

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        g++-mingw-w64-x86-64 unshield xvfb \
    && rm -rf /var/lib/apt/lists/*

COPY sdk/JVLinkSetup.exe /tmp/JVLinkSetup.exe
COPY container/jvlink_demo.cpp /tmp/jvlink_demo.cpp
COPY container/pristine.reg /tmp/pristine.reg

RUN Xvfb "$DISPLAY" -screen 0 1280x800x24 -nolisten tcp >/tmp/xvfb-build.log 2>&1 & \
    sleep 1 \
    && x86_64-w64-mingw32-g++ \
      -std=c++20 -Os -Wall -Wextra -Werror -municode -static -s \
      /tmp/jvlink_demo.cpp -lole32 -loleaut32 -luuid \
      -o /tmp/jvlink-demo.exe \
    && mkdir -p "$WINEPREFIX" /tmp/JVPayload \
    && wineboot --update \
    && timeout 600 wine /tmp/JVLinkSetup.exe '/extract_all:C:\JVExtract' \
    && cabinet=$(find "$WINEPREFIX/drive_c/JVExtract" -type f -name data1.cab -print -quit) \
    && test -n "$cabinet" \
    && unshield x -d /tmp/JVPayload "$cabinet" >/dev/null \
    && install_dir="$WINEPREFIX/drive_c/Program Files/JRA-VAN/Data Lab" \
    && mkdir -p "$install_dir" "$WINEPREFIX/drive_c/JVClient" "$WINEPREFIX/drive_c/JVData" \
    && cp /tmp/JVPayload/JV-Link/JVDTLab.dll "$install_dir/" \
    && cp /tmp/JVPayload/JV-Link64Agent/JVLink64Agent.exe "$install_dir/" \
    && cp /tmp/JVPayload/JV-LinkEnv/* "$install_dir/" \
    && cp /tmp/JVPayload/Remove/msvcp120.dll /tmp/JVPayload/Remove/msvcr120.dll "$install_dir/" \
    && gui=$(find "$install_dir" -maxdepth 1 -type f -name 'JV-Link*.exe' -print -quit) \
    && test -n "$gui" \
    && mv "$gui" "$install_dir/JV-Link.exe" \
    && cp /tmp/jvlink-demo.exe "$WINEPREFIX/drive_c/JVClient/" \
    && wine regsvr32 /s 'C:\Program Files\JRA-VAN\Data Lab\JVDTLab.dll' \
    && wine reg import 'Z:\tmp\pristine.reg' /reg:64 \
    && wineserver --kill \
    && rm -rf /tmp/JVPayload "$WINEPREFIX/drive_c/JVExtract" /tmp/JVLinkSetup.exe /tmp/jvlink_demo.cpp /tmp/pristine.reg

FROM wine-runtime

ENV DISPLAY=:99 \
    PYTHONUNBUFFERED=1 \
    WINEPREFIX=/var/lib/jvlink/prefix

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        nginx novnc python3 tar websockify x11vnc xvfb \
    && rm -rf /var/lib/apt/lists/* /var/log/nginx \
    && mkdir -p /var/lib/jvlink /tmp/client_body /tmp/proxy
RUN apt-get update \
    && apt-get install --yes --no-install-recommends fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/*

COPY --from=prefix-builder /opt/jvlink/pristine-prefix /opt/jvlink/pristine-prefix
COPY container/acquisition.py container/server.py container/state.sh container/entrypoint.sh container/nginx.conf /opt/jvlink/
RUN chmod 700 /opt/jvlink/state.sh /opt/jvlink/entrypoint.sh

EXPOSE 8080
ENTRYPOINT ["/opt/jvlink/entrypoint.sh"]
