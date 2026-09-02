// Run with bun. Prevent deployment when an exposed Worker operation loses JV-Link compatibility.

import {
  JVLINK_DEPLOYED_METHODS,
  JVLINK_DEPLOYMENT_COMPATIBILITY,
  JVLINK_METHODS,
} from "../src/compatibility";

if (!JVLINK_DEPLOYMENT_COMPATIBILITY)
  throw new Error("Deployment contains a JV-Link method without compatibility evidence");

console.log(
  JSON.stringify({
    compatibilityGate: true,
    deployableMethods: JVLINK_DEPLOYED_METHODS.length,
    protocolResearchMethodsExcluded: JVLINK_METHODS.filter(
      ({ level }) => level === "protocol-research-required",
    ).length,
  }),
);
