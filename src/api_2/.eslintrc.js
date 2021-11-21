module.exports = {
    "env": {
        "browser": true,
        "commonjs": true,
        "es6": true
    },
    "extends": "eslint:recommended",
    "globals": {
        "Atomics": "readonly",
        "SharedArrayBuffer": "readonly",
        "process": "readonly"
    },
    "parserOptions": {
        "ecmaVersion": 2018
    },
    "rules": {
        "array-callback-return": 1,
        "no-magic-numbers": 1,
        "no-param-reassign": 1,
        "no-return-assign": 1,
        "no-return-await": 1,
        "no-self-compare": 1,
        "no-unmodified-loop-condition": 1,
        "no-unused-expressions": 1,
        "no-useless-catch": 1,
        "prefer-promise-reject-errors": 1,
        "require-await": 1,
        "no-shadow": 1
    }
};
