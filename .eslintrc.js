module.exports = {
    "env": {
        "es2021": true,
        "node": true
    },
    "parser": "@typescript-eslint/parser",
    "extends": ["standard-with-typescript", "prettier"],
    "plugins": ["prettier"],
    "overrides": [
        {
            "env": {
                "node": true
            },
            "files": [
                ".eslintrc.{js,cjs}"
            ],
            "parserOptions": {
                "sourceType": "script"
            }
        }
    ],
    "parserOptions": {
        "ecmaVersion": "latest",
        "sourceType": "module",
        "project": "./tsconfig.json"
    },
    "rules": {
        "prettier/prettier": ["error"]
    }
}
