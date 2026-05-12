export { PaddockRoom } from "./worker/paddock-room";

// @ts-ignore OpenNext generates this file before Wrangler bundles the Worker.
import worker from "../.open-next/worker.js";

export default worker;
