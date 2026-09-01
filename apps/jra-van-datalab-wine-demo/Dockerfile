FROM debian:trixie-slim

ARG PYTHON_VERSION=3.13.7
ARG PYTHON_INSTALLER_SHA256=b12e2e82461ac8e51fc43289050bc8eb937a32d84ce4d242e2c88258c37cf2bb
ENV DEBIAN_FRONTEND=noninteractive \
    WINEARCH=win64 \
    WINEDEBUG=-all \
    WINEPREFIX=/opt/wineprefix \
    DISPLAY=:99

RUN dpkg --add-architecture i386 \
    && apt-get update \
    && apt-get install --yes --no-install-recommends \
        ca-certificates curl wine wine32 wine64 xauth xvfb \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get update \
    && apt-get install --yes --no-install-recommends unshield \
    && rm -rf /var/lib/apt/lists/*

COPY sdk/JVLinkSetup.exe sdk/JVStartup.exe sdk/PROMID.DAT sdk/ShiftVISTA_Check.exe sdk/ShiftVISTA_Update.exe /tmp/JVLink/

RUN mkdir -p "$WINEPREFIX" \
    && xvfb-run --auto-servernum wineboot --update \
    && cd /tmp/JVLink \
    && timeout 600 xvfb-run --auto-servernum wine ./JVLinkSetup.exe '/extract_all:C:\JVExtract' \
    && cabinet=$(find "$WINEPREFIX/drive_c/JVExtract" -type f -name data1.cab -print -quit) \
    && test -n "$cabinet" \
    && unshield x -d /tmp/JVPayload "$cabinet" \
    && install_dir="$WINEPREFIX/drive_c/Program Files/JRA-VAN/Data Lab" \
    && mkdir -p "$install_dir" \
    && cp /tmp/JVPayload/JV-Link/JVDTLab.dll "$install_dir/" \
    && cp /tmp/JVPayload/JV-Link64Agent/JVLink64Agent.exe "$install_dir/" \
    && xvfb-run --auto-servernum wine regsvr32 /s 'C:\Program Files\JRA-VAN\Data Lab\JVDTLab.dll' \
    && xvfb-run --auto-servernum wine reg query 'HKCR\JVDTLab.JVLink' \
    && rm -rf /tmp/JVLink /tmp/JVPayload "$WINEPREFIX/drive_c/JVExtract"

RUN curl --fail --location --output /tmp/python.exe \
        "https://www.python.org/ftp/python/${PYTHON_VERSION}/python-${PYTHON_VERSION}-amd64.exe" \
    && echo "${PYTHON_INSTALLER_SHA256}  /tmp/python.exe" | sha256sum --check --strict \
    && timeout 600 xvfb-run --auto-servernum wine /tmp/python.exe /quiet \
        InstallAllUsers=1 TargetDir=C:\\Python313 Include_test=0 Include_launcher=0 \
    && wineserver --wait \
    && timeout 600 xvfb-run --auto-servernum wine 'C:\Python313\python.exe' \
        -m pip install --no-cache-dir pywin32==311 \
    && wineserver --wait \
    && rm /tmp/python.exe

WORKDIR /app
COPY src/ /app/src/
COPY scripts/entrypoint.sh /app/scripts/entrypoint.sh
RUN chmod +x /app/scripts/entrypoint.sh && mkdir -p /data

ENTRYPOINT ["/app/scripts/entrypoint.sh"]
