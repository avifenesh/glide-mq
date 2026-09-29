// Entry for `npm run compat:deno`. The scenario lives in smoke-core.mts.
import { runSmoke } from './smoke-core.mts';

runSmoke().then((code) => process.exit(code));
