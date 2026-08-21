export default async function luaCoverageTeardown(): Promise<void> {
  if (process.env.GLIDEMQ_LUA_COVERAGE !== '1') return;
  const { dumpLuaCoverage } = await import('./lua-coverage');
  await dumpLuaCoverage();
}
