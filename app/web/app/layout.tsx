import type { Metadata } from "next";
import Providers from "./components/Providers";
import "./globals.css";

export const metadata: Metadata = {
  title: "DCar Insight · 内容运营工作台",
  description: "本地优先的账号、内容、卖点、抓取和报告运营工作流。",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="zh-CN">
      <body><Providers>{children}</Providers></body>
    </html>
  );
}
