"use client";

import { createContext, useContext } from "react";
import type { Section } from "../lib/types";
import { dataServiceStatus, type ServiceState } from "../lib/serviceStatus";

type WorkbenchState = { activeSection: Section | null; serviceState: ServiceState };
const defaultState: WorkbenchState = {
  activeSection: null,
  serviceState: dataServiceStatus(undefined, false),
};

export const WorkbenchContext = createContext<WorkbenchState>(defaultState);
export function useWorkbench() { return useContext(WorkbenchContext); }

/** Only known workbench routes receive chrome; login and unknown routes stay independent. */
export function workbenchSection(pathname: string | null): Section | null {
  const path = pathname?.replace(/\/+$/, "") ?? "";
  const primary = /^\/(overview|contents|accounts|selling-points|spu-audience|tasks|users)$/.exec(path);
  if (primary) return primary[1] as Section;
  if (/^\/tasks\/[^/]+$/.test(path)) return "tasks";
  if (path === "/accounts/douyin-authorization") return "accounts";
  return null;
}
