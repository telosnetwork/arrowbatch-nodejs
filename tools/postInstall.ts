import path from "node:path";
import fs from "node:fs";
import {packageInfo, ROOT_DIR} from "../src/index.js";

const binPath = path.join(ROOT_DIR, 'build', 'tools');
const nodeModulesBinPath = path.join(ROOT_DIR, 'node_modules', '.bin');

const binaries = Object.values(packageInfo.bin).map((binPath: string) => path.basename(binPath));

fs.readdirSync(binPath).forEach(file => {
    if (!binaries.includes(file))
        return;

    const srcPath = path.join(binPath, file);

    if (!fs.existsSync(srcPath))
        throw new Error(`source binary does not exist!`);

    const srcStat = fs.statSync(srcPath);

    fs.chmodSync(srcPath, srcStat.mode | fs.constants.S_IXUSR);

    const destPath = path.join(nodeModulesBinPath, file);

    if (fs.existsSync(destPath))
        fs.rmSync(destPath);

    fs.symlinkSync(srcPath, destPath, 'file');
});