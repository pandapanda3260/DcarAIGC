export function canAccessAccounts(role: string | undefined) {
  return role === "admin" || role === "superadmin";
}
