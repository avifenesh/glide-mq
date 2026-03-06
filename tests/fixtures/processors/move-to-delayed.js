module.exports = async function moveToDelayed(job) {
  await job.moveToDelayed(123456, 'next');
};
