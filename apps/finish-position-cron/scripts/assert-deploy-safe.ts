// Run with bun. Fail closed when a finish-position Container instance is live.

import {
  describeUnsafePredictionContainers,
  listUnsafePredictionContainers,
} from "./wrangler-container-state";

const unsafe = await listUnsafePredictionContainers();

if (unsafe.length > 0) {
  const detail = describeUnsafePredictionContainers(unsafe);
  throw new Error(`Prediction Containers are active; refusing deployment: ${detail}`);
}

console.log("[deploy-safety] all finish-position Container instances are inactive");
