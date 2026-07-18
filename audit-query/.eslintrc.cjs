module.exports = {
  root: true,
  env: {
    node: true,
    es2021: true,
    commonjs: true
  },
  extends: ['eslint:recommended', 'plugin:prettier/recommended'],
  parserOptions: {
    sourceType: 'module',
    ecmaVersion: 'latest'
  },
  plugins: ['prettier'],
  rules: {
    'no-console': 'off',
    'consistent-return': 'off',
    'no-debugger': process.env.NODE_ENV === 'production' ? 'error' : 'warn',
    'prettier/prettier': 'error',
    'no-unused-vars': [
      'error',
      {
        argsIgnorePattern: '^_',
        varsIgnorePattern: '^_',
        caughtErrors: 'none'
      }
    ],
    'prefer-const': 'error',
    'no-var': 'error',
    'no-shadow': 'error',
    eqeqeq: ['error', 'always'],
    curly: ['error', 'all'],
    'no-throw-literal': 'error',
    'prefer-promise-reject-errors': 'error',
    'object-shorthand': 'error',
    'prefer-template': 'error',
    'template-curly-spacing': ['error', 'never'],
    'no-path-concat': 'error',
    'handle-callback-err': 'error',
    'arrow-body-style': ['error', 'as-needed'],
    'prefer-arrow-callback': 'error',
    'prefer-destructuring': [
      'error',
      {
        array: false,
        object: true
      }
    ],
    semi: 'off',
    quotes: 'off',
    indent: 'off',
    'comma-dangle': 'off'
  },
  overrides: [
    {
      files: ['scripts/**/*.js'],
      rules: {
        'no-process-exit': 'off'
      }
    },
    {
      files: ['**/*.test.js', 'tests/**/*.js'],
      env: {
        jest: true
      },
      rules: {
        'no-unused-expressions': 'off'
      }
    }
  ]
}
