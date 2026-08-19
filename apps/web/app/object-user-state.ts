export const OBJECT_USER_STATE_TYPES = ["item", "report", "topic"] as const;

export type ObjectUserStateType = (typeof OBJECT_USER_STATE_TYPES)[number];

export function isObjectUserStateType(value: unknown): value is ObjectUserStateType {
  return OBJECT_USER_STATE_TYPES.includes(value as ObjectUserStateType);
}
