var java = require("java");
java.classpath.push("./src");

var Atosl = java.newInstanceSync("io.honeyqa.atosl.Atosl");

module.exports = {
    getArch: function(path) {
        var result = [];
        var rawArch = Atosl.getArchSync(path);
        if (rawArch != null) {
            for (var i = 0; i < rawArch.sizeSync(); i++) {
                result.push(rawArch.getSync(i));
            }
            return result;
        } else {
            return null;
        }
    },
    getUUID: function(arch, path) {
        return Atosl.getUUIDSync(arch, path);
    },
    symbolicate: function(arch, path, addresses) {
        var result = [];
        var converted = java.newArray('java.lang.String', addresses);
        var rawSymbolicated = Atosl.doSymbolicateSync(arch, path, converted);
        if (rawSymbolicated != null) {
            for (var i = 0; i < rawSymbolicated.sizeSync(); i++) {
                result.push(rawSymbolicated.getSync(i));
            }
            return result;
        } else {
            return null;
        }
    }
}
