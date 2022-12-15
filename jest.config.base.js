module.exports = {
    roots: ['<rootDir>'],
    preset: "babel-jest",
    transform: {
        '^.+\\.tsx?$': 'babel-jest',
    },
    testRegex: '(__tests__/.*(\\.|/)(test|spec))\\.(js|ts)$',
    moduleFileExtensions: ['ts', 'js', 'json', 'node'],
    moduleNameMapper: {
        '^@ethql/(.*)$': '<rootDir>/../$1/',
    },
    verbose: true,
    setupTestFrameworkScriptFile: __dirname + '/jest.setup.js',
    testEnvironment: 'node',
};
