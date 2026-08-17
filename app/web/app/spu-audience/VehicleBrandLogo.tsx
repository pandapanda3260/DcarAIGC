"use client";

import { useState } from "react";
import Image from "next/image";
import { CarIcon } from "@phosphor-icons/react";
import { vehicleBrandLogoPath } from "./vehicleBrandLogos";

export function VehicleBrandLogo({ brand }: { brand: string }) {
  const source = vehicleBrandLogoPath(brand);
  const [failedSource, setFailedSource] = useState<string | null>(null);
  const showFallback = source == null || failedSource === source;

  return (
    <div className="vehicle-brand-logo" aria-hidden="true">
      {showFallback ? (
        <CarIcon size={16} weight="regular" />
      ) : (
        <Image
          className="vehicle-brand-logo-image"
          src={source}
          alt=""
          width={28}
          height={24}
          unoptimized
          onError={() => setFailedSource(source)}
        />
      )}
    </div>
  );
}
