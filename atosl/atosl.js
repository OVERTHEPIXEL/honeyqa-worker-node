var java = require("java");
java.classpath.push("./src");

var Atosl = java.newInstanceSync("io.honeyqa.atosl.Atosl");

module.exports = {
    getArch: function(path){
        var result = [];
        var rawArch = Atosl.getArchSync(path);
        for (var i = 0; i < rawArch.sizeSync(); i++) {
            result.push(rawArch.getSync(i));
        }
        return result;
    }
}
