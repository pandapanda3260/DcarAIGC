"use client";

import { QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { getQueryClient } from "../lib/queryClient";
import ContentUpdateJobsProvider from "./ContentUpdateJobsProvider";
import WorkbenchChrome from "./WorkbenchChrome";

export default function Providers({ children }: { children: ReactNode }) {
  const queryClient = getQueryClient();
  return <QueryClientProvider client={queryClient}><ContentUpdateJobsProvider><WorkbenchChrome>{children}</WorkbenchChrome></ContentUpdateJobsProvider></QueryClientProvider>;
}
