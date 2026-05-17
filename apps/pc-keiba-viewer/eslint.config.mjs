import css from "@eslint/css";
import { defineConfig } from "eslint/config";

const cssLiteralPattern =
  /(?:#[0-9a-f]{3,8}\b|(?:rgb|rgba|hsl|hsla|hwb|lab|lch|oklab|oklch)\(|(?<![-\w.])-?\d*\.?\d+(?:px|rem|em|vh|vw|vmin|vmax|dvh|svh|lvh|%|deg|turn|rad|s|ms|fr|ch|lh|rlh|cqw|cqh|cqi|cqb|cqmin|cqmax)?\b)/iu;

const stripAllowedSegments = (value) =>
  value
    .replace(/var\((?:[^()]|\([^()]*\))*\)/giu, "")
    .replace(/url\((?:[^()]|\([^()]*\))*\)/giu, "")
    .replace(/"[^"]*"|'[^']*'/gu, "");

const cssVarsPlugin = {
  meta: {
    name: "pc-keiba-css-vars",
  },
  rules: {
    "require-vars": {
      meta: {
        type: "problem",
        docs: {
          description:
            "Require CSS declaration numbers and colors to be referenced through vars.css.",
        },
        messages: {
          literal:
            "CSS declaration values must reference numbers and colors through CSS variables from vars.css.",
        },
      },
      create(context) {
        const sourceCode = context.sourceCode;

        return {
          "Rule > Block Declaration:exit"(node) {
            const valueText = sourceCode.getText(node.value);
            const textToCheck = stripAllowedSegments(valueText);

            if (!cssLiteralPattern.test(textToCheck)) {
              return;
            }

            context.report({
              loc: node.value.loc,
              messageId: "literal",
            });
          },
        };
      },
    },
  },
};

export default defineConfig([
  {
    files: ["src/app/**/*.css"],
    language: "css/css",
    plugins: {
      css,
      "pc-keiba-css-vars": cssVarsPlugin,
    },
    extends: ["css/recommended"],
    rules: {
      "css/no-invalid-properties": "off",
      "css/use-baseline": "off",
      "pc-keiba-css-vars/require-vars": "error",
    },
  },
  {
    files: ["src/app/vars.css"],
    rules: {
      "pc-keiba-css-vars/require-vars": "off",
    },
  },
]);
