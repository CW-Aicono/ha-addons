"use strict";
/**
 * Modbus-TCP ↔ OCPP-1.6J Bridge
 * Phase 5: per Wallbox eine eigene OCPP-Verbindung zum Cloud-Backend.
 *
 * Templates definieren read_map / write_map / status_map (siehe
 * wallbox_modbus_templates Tabelle). Eine Bridge-Instanz pollt die
 * Modbus-Register laut Template, übersetzt Werte in OCPP MeterValues /
 * StatusNotifications und mapped Befehle (RemoteStart/Stop, ChangeConfiguration)
 * zurück auf Modbus-Writes.
 *
 * Hinweis: dieses Modul vermeidet Top-Level externe Imports. Das `modbus-serial`
 * Package wird im HA-Addon-Container per dynamic require geladen, damit der
 * Cloud-Build (Lovable) ohne node_modules nicht bricht.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.WallboxBridgeManager = exports.ModbusWallboxBridge = void 0;
const OCPP_PROTOCOL = "ocpp1.6";
function decode(buf, entry) {
    const big = entry.byte_order !== "little";
    const word = (idx) => buf[idx] & 0xffff;
    switch (entry.data_type) {
        case "uint16": return word(0);
        case "int16": {
            const v = word(0);
            return v >= 0x8000 ? v - 0x10000 : v;
        }
        case "uint32": return big
            ? (word(0) << 16) | word(1)
            : (word(1) << 16) | word(0);
        case "int32": {
            const v = big ? (word(0) << 16) | word(1) : (word(1) << 16) | word(0);
            return v >= 0x80000000 ? v - 0x100000000 : v;
        }
        case "float32": {
            const ab = new ArrayBuffer(4);
            const dv = new DataView(ab);
            if (big) {
                dv.setUint16(0, word(0));
                dv.setUint16(2, word(1));
            }
            else {
                dv.setUint16(0, word(1));
                dv.setUint16(2, word(0));
            }
            return dv.getFloat32(0);
        }
        case "float64": {
            const ab = new ArrayBuffer(8);
            const dv = new DataView(ab);
            const order = big ? [0, 1, 2, 3] : [3, 2, 1, 0];
            for (let i = 0; i < 4; i++)
                dv.setUint16(i * 2, word(order[i]));
            return dv.getFloat64(0);
        }
        case "string": {
            const chars = [];
            for (const w of buf) {
                chars.push(String.fromCharCode((w >> 8) & 0xff));
                chars.push(String.fromCharCode(w & 0xff));
            }
            return chars.join("").replace(/\0+$/, "").trim();
        }
    }
}
function encode(value, entry) {
    switch (entry.data_type) {
        case "uint16":
        case "int16":
            return [value & 0xffff];
        case "uint32":
        case "int32":
            return [(value >>> 16) & 0xffff, value & 0xffff];
        case "float32": {
            const ab = new ArrayBuffer(4);
            const dv = new DataView(ab);
            dv.setFloat32(0, value);
            return [dv.getUint16(0), dv.getUint16(2)];
        }
    }
}
class ModbusWallboxBridge {
    inst;
    tpl;
    cloudUrl;
    cloudPassword;
    modbus = null;
    ws = null;
    pollFast;
    pollSlow;
    state = {};
    lastOcppStatus = "Available";
    msgId = 1;
    callbacks = new Map();
    transactionId = null;
    stopped = false;
    constructor(inst, tpl, cloudUrl, cloudPassword) {
        this.inst = inst;
        this.tpl = tpl;
        this.cloudUrl = cloudUrl;
        this.cloudPassword = cloudPassword;
    }
    async start() {
        await this.connectModbus();
        this.connectOcpp();
        const fast = this.tpl.poll_intervals?.fast_ms ?? 3000;
        const slow = this.tpl.poll_intervals?.slow_ms ?? 30000;
        this.pollFast = setInterval(() => this.pollGroup("fast").catch((e) => console.error("[wb-bridge] fast", e?.message)), fast);
        this.pollSlow = setInterval(() => this.pollGroup("slow").catch((e) => console.error("[wb-bridge] slow", e?.message)), slow);
    }
    async stop() {
        this.stopped = true;
        if (this.pollFast)
            clearInterval(this.pollFast);
        if (this.pollSlow)
            clearInterval(this.pollSlow);
        try {
            this.ws?.close();
        }
        catch { /* ignore */ }
        try {
            await this.modbus?.close?.();
        }
        catch { /* ignore */ }
    }
    async connectModbus() {
        // Dynamisch laden, damit Cloud-Build ohne modbus-serial nicht bricht.
        let ModbusRTU;
        try {
            // eslint-disable-next-line @typescript-eslint/no-var-requires
            ModbusRTU = require("modbus-serial");
        }
        catch (e) {
            throw new Error(`modbus-serial package missing in addon: ${e.message}`);
        }
        this.modbus = new ModbusRTU();
        await this.modbus.connectTCP(this.inst.modbus_host, { port: this.inst.modbus_port });
        this.modbus.setID(this.inst.unit_id);
        this.modbus.setTimeout(2000);
        console.log(`[wb-bridge] Modbus connected ${this.inst.modbus_host}:${this.inst.modbus_port} unit=${this.inst.unit_id}`);
    }
    connectOcpp() {
        const auth = "Basic " + Buffer.from(`${this.inst.charge_point_ocpp_id}:${this.cloudPassword}`).toString("base64");
        const url = `${this.cloudUrl.replace(/\/$/, "")}/${this.inst.charge_point_ocpp_id}`;
        // Use global WebSocket if available (Deno/Bun); otherwise dynamic require ws.
        const WS = globalThis.WebSocket ?? require("ws");
        this.ws = new WS(url, OCPP_PROTOCOL, { headers: { Authorization: auth } });
        this.ws.onopen = () => {
            console.log(`[wb-bridge] OCPP connected ${this.inst.charge_point_ocpp_id}`);
            this.sendCall("BootNotification", {
                chargePointVendor: this.tpl.vendor.slice(0, 20),
                chargePointModel: this.tpl.model.slice(0, 20),
                firmwareVersion: "modbus-bridge-v1",
            });
        };
        this.ws.onmessage = (ev) => this.handleOcppFrame(ev.data);
        this.ws.onclose = () => {
            if (this.stopped)
                return;
            console.warn(`[wb-bridge] OCPP closed for ${this.inst.charge_point_ocpp_id}, reconnecting in 5s`);
            setTimeout(() => this.connectOcpp(), 5000);
        };
        this.ws.onerror = (err) => console.error("[wb-bridge] ws error", err?.message);
    }
    sendCall(action, payload) {
        if (!this.ws || this.ws.readyState !== 1)
            return;
        const id = `wb-${this.msgId++}`;
        const frame = JSON.stringify([2, id, action, payload]);
        this.ws.send(frame);
    }
    async handleOcppFrame(raw) {
        try {
            const text = typeof raw === "string" ? raw : new TextDecoder().decode(raw);
            const arr = JSON.parse(text);
            if (!Array.isArray(arr))
                return;
            const [type, msgId, payload, payload2] = arr;
            if (type === 3) {
                const cb = this.callbacks.get(msgId);
                if (cb) {
                    this.callbacks.delete(msgId);
                    cb(payload);
                }
                return;
            }
            if (type === 2) {
                const action = payload;
                const data = payload2 ?? {};
                const resp = await this.handleRemoteCall(action, data);
                this.ws?.send(JSON.stringify([3, msgId, resp]));
            }
        }
        catch (e) {
            console.error("[wb-bridge] frame error", e.message);
        }
    }
    async handleRemoteCall(action, data) {
        switch (action) {
            case "RemoteStartTransaction":
                await this.startCharge();
                return { status: "Accepted" };
            case "RemoteStopTransaction":
                await this.stopCharge();
                return { status: "Accepted" };
            case "ChangeConfiguration":
                if ((data?.key || "").toLowerCase().includes("current")) {
                    await this.setCurrent(Number(data.value));
                    return { status: "Accepted" };
                }
                return { status: "NotSupported" };
            case "Reset":
                return { status: "Accepted" };
            case "TriggerMessage":
                return { status: "Accepted" };
            default:
                return { status: "NotImplemented" };
        }
    }
    async pollGroup(group) {
        const entries = (this.tpl.read_map ?? []).filter((e) => (e.poll_group ?? "fast") === group);
        for (const entry of entries) {
            try {
                const fn = entry.function_code === 3 ? this.modbus.readHoldingRegisters : this.modbus.readInputRegisters;
                const len = entry.data_type === "uint32" || entry.data_type === "int32" || entry.data_type === "float32"
                    ? 2
                    : entry.data_type === "float64"
                        ? 4
                        : entry.data_type === "string"
                            ? (entry.length ?? 4)
                            : 1;
                const res = await fn.call(this.modbus, entry.address, len);
                const raw = decode(res.data, entry);
                const scaled = typeof raw === "number" && entry.scale != null ? raw * entry.scale : raw;
                this.state[entry.target_field] = scaled;
            }
            catch (e) {
                console.warn(`[wb-bridge] read fail @${entry.address}`, e.message);
            }
        }
        if (group === "fast")
            this.pushStatusAndMeter();
    }
    pushStatusAndMeter() {
        // Map vendor status -> OCPP status
        const vs = String(this.state.vendor_status ?? "");
        const ocppStatus = this.tpl.status_map?.[vs] ?? "Available";
        if (ocppStatus !== this.lastOcppStatus) {
            this.lastOcppStatus = ocppStatus;
            this.sendCall("StatusNotification", {
                connectorId: 1,
                status: ocppStatus,
                errorCode: "NoError",
                timestamp: new Date().toISOString(),
            });
            // auto start/stop transaction tracking
            if (ocppStatus === "Charging" && this.transactionId == null) {
                this.transactionId = Math.floor(Date.now() / 1000);
                this.sendCall("StartTransaction", {
                    connectorId: 1,
                    idTag: "GATEWAY",
                    meterStart: Math.round((this.state.energy_total_kwh ?? 0) * 1000),
                    timestamp: new Date().toISOString(),
                });
            }
            else if (ocppStatus !== "Charging" && this.transactionId != null) {
                this.sendCall("StopTransaction", {
                    transactionId: this.transactionId,
                    meterStop: Math.round((this.state.energy_total_kwh ?? 0) * 1000),
                    timestamp: new Date().toISOString(),
                });
                this.transactionId = null;
            }
        }
        // MeterValues
        if (this.state.power_total_w != null || this.state.energy_total_kwh != null) {
            const sampledValue = [];
            if (this.state.power_total_w != null) {
                sampledValue.push({
                    value: String(this.state.power_total_w),
                    measurand: "Power.Active.Import",
                    unit: "W",
                });
            }
            if (this.state.energy_total_kwh != null) {
                sampledValue.push({
                    value: String(Math.round(this.state.energy_total_kwh * 1000)),
                    measurand: "Energy.Active.Import.Register",
                    unit: "Wh",
                });
            }
            this.sendCall("MeterValues", {
                connectorId: 1,
                transactionId: this.transactionId ?? undefined,
                meterValue: [{ timestamp: new Date().toISOString(), sampledValue }],
            });
        }
    }
    async setCurrent(amps) {
        const w = this.tpl.write_map?.set_current;
        if (!w)
            throw new Error("Template has no set_current");
        const clamped = Math.min(Math.max(amps, w.min ?? 6), w.max ?? 32);
        const scaled = Math.round(clamped * (w.scale ?? 1));
        const regs = encode(scaled, w);
        if (w.function_code === 6) {
            await this.modbus.writeRegister(w.address, regs[0]);
        }
        else {
            await this.modbus.writeRegisters(w.address, regs);
        }
    }
    async startCharge() {
        const w = this.tpl.write_map?.start_charge;
        if (!w)
            return;
        await this.modbus.writeRegister(w.address, w.value ?? 1);
    }
    async stopCharge() {
        const w = this.tpl.write_map?.stop_charge;
        if (!w)
            return;
        await this.modbus.writeRegister(w.address, w.value ?? 0);
    }
    getState() { return { ...this.state, ocpp_status: this.lastOcppStatus }; }
}
exports.ModbusWallboxBridge = ModbusWallboxBridge;
/** Bridge-Manager: hält alle Bridges des Gateways. */
class WallboxBridgeManager {
    cloudWsUrl;
    cloudPassword;
    bridges = new Map();
    constructor(cloudWsUrl, cloudPassword) {
        this.cloudWsUrl = cloudWsUrl;
        this.cloudPassword = cloudPassword;
    }
    async provision(inst, tpl) {
        await this.remove(inst.id);
        const bridge = new ModbusWallboxBridge(inst, tpl, this.cloudWsUrl, this.cloudPassword);
        await bridge.start();
        this.bridges.set(inst.id, bridge);
    }
    async remove(id) {
        const b = this.bridges.get(id);
        if (b) {
            await b.stop();
            this.bridges.delete(id);
        }
    }
    list() { return [...this.bridges.keys()]; }
    get(id) { return this.bridges.get(id); }
}
exports.WallboxBridgeManager = WallboxBridgeManager;
