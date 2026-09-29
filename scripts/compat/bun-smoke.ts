// Entry for `npm run compat:bun`. The scenario lives in smoke-core.mts.
import { runSmoke } from './smoke-core.mts';

runSmoke().then((code) => process.exit(code));
