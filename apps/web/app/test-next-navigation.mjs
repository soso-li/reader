const router = {
  back() {},
  forward() {},
  prefetch() {},
  push() {},
  refresh() {},
  replace() {}
};

export const testRouter = router;

export function useRouter() {
  return router;
}

export function redirect(url) {
  throw new Error(`redirect:${url}`);
}
