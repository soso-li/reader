type MobilePane = "sources" | "list" | "detail";

export function setMobilePane(pane: MobilePane) {
  const shell = document.querySelector(".app-shell");
  shell?.classList.toggle("mobile-list", pane === "list");
  shell?.classList.toggle("mobile-detail", pane === "detail");
}

export function setMobileDetail(show: boolean) {
  if (show) {
    setMobilePane("detail");
  } else {
    document.querySelector(".app-shell")?.classList.remove("mobile-detail");
  }
}

export function setMobileList(show: boolean) {
  if (show) {
    setMobilePane("list");
  } else {
    document.querySelector(".app-shell")?.classList.remove("mobile-list");
  }
}
