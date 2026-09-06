#!/usr/bin/env bun

import { assertLegacyReplicaWriteEnabled } from "../src/replica-push/legacy-replica-authority";

assertLegacyReplicaWriteEnabled(Bun.env.ALLOW_LEGACY_REPLICA_WRITE);
