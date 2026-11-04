import { expect, it } from "vitest";
import { phase, vnToday } from "./session";

const at = (iso: string) => new Date(iso); // UTC instants

it("uses Vietnam time, not the viewer's clock", () => {
  expect(vnToday(at("2026-11-04T17:30:00Z"))).toBe("2026-11-05");
  expect(vnToday(at("2026-11-04T01:45:00Z"))).toBe("2026-11-04");
});

it("phases", () => {
  expect(phase(at("2026-11-04T01:59:00Z"))).toBe("pre"); // 08:59 VN
  expect(phase(at("2026-11-04T02:00:00Z"))).toBe("open"); // 09:00
  expect(phase(at("2026-11-04T04:45:00Z"))).toBe("lunch"); // 11:45
  expect(phase(at("2026-11-04T06:00:00Z"))).toBe("open"); // 13:00
  expect(phase(at("2026-11-04T07:55:00Z"))).toBe("closing"); // 14:55
  expect(phase(at("2026-11-04T08:30:00Z"))).toBe("after"); // 15:30
  expect(phase(at("2026-11-07T03:00:00Z"))).toBe("closed"); // Saturday
});
