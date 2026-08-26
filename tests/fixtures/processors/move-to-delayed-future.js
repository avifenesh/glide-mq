module.exports = async function moveToDelayedFuture(job) {
  await job.moveToDelayed(Date.now() + 60_000);
};
