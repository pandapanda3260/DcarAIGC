import type { Section } from "../app/lib/types";

export type NavigationFeedbackSnapshot = Readonly<{ id: number | string; href: string; section: Section }> | null;
export type NavigationFeedbackStore = {
  start(id: number | string, href: string, basePath?: string, navigationKind?: string): void;
  finish(id: number | string): void;
  subscribe(listener: () => void): () => void;
  getSnapshot(): NavigationFeedbackSnapshot;
  getServerSnapshot(): null;
};
export function createNavigationFeedbackStore(): NavigationFeedbackStore;
export const navigationFeedbackStore: NavigationFeedbackStore;
