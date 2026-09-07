"use client";

import { QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { getQueryClient } from "../lib/queryClient";
import ContentUpdateJobsProvider from "./ContentUpdateJobsProvider";

export default function Providers({ children }: { children: ReactNode }) {
  const queryClient = getQueryClient();
  return <QueryClientProvider client={queryClient}><ContentUpdateJobsProvider>{children}</ContentUpdateJobsProvider></QueryClientProvider>;
}
