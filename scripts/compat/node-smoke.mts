// Entry for `npm run compat:node`. The scenario lives in smoke-core.mts.
import { runSmoke } from './smoke-core.mts';

runSmoke().then((code) => process.exit(code));
