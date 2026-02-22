module.exports = async (job) => {
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve({ aborted: false }), 30000);
    job.abortSignal.addEventListener('abort', () => {
      clearTimeout(timer);
      resolve({ aborted: true });
    });
  });
};
