export function convertGermanNumberToNumber(
  str: string | null,
): number | string | null {
  if (str == null) return str;
  if (str.search(/,/) < 0) {
    return str;
  }
  const stringWithDot = str.replace(",", ".");
  // Parse the string to a floating-point number
  return parseFloat(stringWithDot);
}

export function convertHyphenToNull(str?: string | null): string | null {
  if (str === "-" || str === "-;") {
    return null;
  }

  if (typeof str === "string") {
    return str;
  }

  return null;
}
