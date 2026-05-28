// run with: bun run test
import { expect, it } from "vitest";
import { parseJraOddsByType } from "./jra";

it("parseJraOddsByType parses fukusho min/max odds with averageOdds", () => {
  const html = `
    <table class="tanpuku">
      <tr><td class="num">1</td><td class="odds_fuku">1.5 - 2.5</td></tr>
      <tr><td class="num">2</td><td class="odds_fuku">3.0 - 5.0</td></tr>
    </table>
  `;
  const result = parseJraOddsByType("fukusho", html);
  expect(result.length).toBe(2);
  expect(result[0]!.combination).toBe("1");
  expect(result[0]!.minOdds).toBe(1.5);
  expect(result[0]!.maxOdds).toBe(2.5);
  expect(result[0]!.averageOdds).toBe(2);
});

it("parseJraOddsByType parses umaren as unordered pairs", () => {
  const html = `
    <table class="umaren">
      <caption>1</caption>
      <tr><th>2</th><td>5.5</td></tr>
      <tr><th>3</th><td>7.5</td></tr>
    </table>
  `;
  const result = parseJraOddsByType("umaren", html);
  expect(result.length).toBe(2);
  expect(result[0]!.combination).toBe("1-2");
});

it("parseJraOddsByType parses umatan as ordered pairs", () => {
  const html = `
    <table class="umatan">
      <caption>3</caption>
      <tr><th>1</th><td>10.5</td></tr>
      <tr><th>2</th><td>15.5</td></tr>
    </table>
  `;
  const result = parseJraOddsByType("umatan", html);
  expect(result.length).toBe(2);
  expect(result[0]!.combination).toBe("3-1");
});

it("parseJraOddsByType parses wakuren and filters invalid frame numbers", () => {
  const html = `
    <table class="waku">
      <caption class="waku1">1</caption>
      <tr><th>2</th><td>3.5</td></tr>
      <tr><th>8</th><td>5.5</td></tr>
    </table>
  `;
  const result = parseJraOddsByType("wakuren", html);
  expect(result.length).toBe(2);
});

it("parseJraOddsByType returns empty array when 3renpuku has no fuku3 table", () => {
  const result = parseJraOddsByType("3renpuku", "<html></html>");
  expect(result).toStrictEqual([]);
});

it("parseJraOddsByType parses 3renpuku from a fuku3 table", () => {
  const html = `
    <table class="fuku3">
      <caption>1-2</caption>
      <tr><th>3</th><td>15.5</td></tr>
      <tr><th>4</th><td>20.5</td></tr>
    </table>
  `;
  const result = parseJraOddsByType("3renpuku", html);
  expect(result.length).toBe(2);
});

it("parseJraOddsByType parses 3rentan from a tan3_unit block", () => {
  const html = `
    <div class="tan3_unit">
      <span class="inner"><span class="num">1</span></span>
      <div class="cap"><span>2着</span></div>
      <div class="num">2</div>
      <table>
        <tr><th>3</th><td>10.0</td></tr>
        <tr><th>4</th><td>20.0</td></tr>
      </table>
    </div>
  `;
  const result = parseJraOddsByType("3rentan", html);
  expect(result.length).toBe(2);
});
