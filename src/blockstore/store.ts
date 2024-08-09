import pLimit from "p-limit";
import { format, parse, ToString } from "@ipld/dag-json";
import { Logger, ResolveOnce, Result, URI } from "@adviser/cement";

import type {
  AnyBlock,
  AnyLink,
  CommitOpts,
  DataSaveOpts,
  DataStore,
  DbMeta,
  MetaStore,
  WALStore as WALStore,
  WALState,
  LoadHandler,
  KeyedCrypto,
  Loadable,
} from "./types.js";
import { Falsy, StoreType, throwFalsy } from "../types.js";
import { Gateway } from "./gateway.js";
import { ensureLogger, exception2Result, isNotFoundError } from "../utils.js";
import { carLogIncludesGroup } from "./loader.js";
import { CommitQueue } from "./commit-queue.js";
import { keyedCryptoFactory } from "../runtime/keyed-crypto.js";
import { KeyBag } from "../runtime/key-bag.js";

function guardVersion(url: URI): Result<URI> {
  if (!url.hasParam("version")) {
    return Result.Err(`missing version: ${url.toString()}`);
  }
  return Result.Ok(url);
}

export interface StoreOpts {
  readonly gateway: Gateway;
  readonly logger: Logger;
  readonly textEncoder?: TextEncoder;
  readonly textDecoder?: TextDecoder;
  readonly keybag: () => Promise<KeyBag>;
}

const _lazyTextEncoder = new ResolveOnce<TextEncoder>();
const _lazyTextDecoder = new ResolveOnce<TextDecoder>();

abstract class BaseStoreImpl {
  // should be injectable
  readonly textEncoder;
  readonly textDecoder;

  abstract readonly storeType: StoreType;
  readonly name: string;

  private _url: URI;
  readonly logger: Logger;
  readonly gateway: Gateway;
  readonly keybag: () => Promise<KeyBag>;
  constructor(name: string, url: URI, opts: StoreOpts) {
    this.name = name;
    this._url = url;
    this.keybag = opts.keybag;
    this.logger = opts.logger
      .With()
      .Ref("url", () => this._url.toString())
      .Str("id", "" + Math.random())
      .Str("name", name)
      .Logger();
    this.gateway = opts.gateway;
    this.textEncoder = opts.textEncoder || _lazyTextEncoder.once(() => new TextEncoder());
    this.textDecoder = opts.textDecoder || _lazyTextDecoder.once(() => new TextDecoder());
  }

  url(): URI {
    return this._url;
  }

  readonly _onStarted: (() => void)[] = [];
  onStarted(fn: () => void) {
    this._onStarted.push(fn);
  }
  readonly _onClosed: (() => void)[] = [];
  onClosed(fn: () => void) {
    this._onClosed.push(fn);
  }
  abstract close(): Promise<Result<void>>;

  readonly ready?: () => Promise<void>;

  async keyedCrypto(): Promise<KeyedCrypto> {
    this.logger.Debug().Msg("keyedCrypto");
    const kb = await this.keybag();
    this.logger.Debug().Msg("keyedCrypto-keybag");
    const kcf = await keyedCryptoFactory(this._url, kb, this.logger);
    this.logger.Debug().Msg("keyedCrypto-kcf");
    return kcf;
  }

  async start(): Promise<Result<URI>> {
    this.logger.Debug().Str("storeType", this.storeType).Msg("starting-gateway-pre");
    this._url = this._url.build().setParam("store", this.storeType).URI();
    const res = await this.gateway.start(this._url);
    if (res.isErr()) {
      this.logger.Error().Result("gw-start", res).Msg("started-gateway");
      return res as Result<URI>;
    }
    this._url = res.Ok();
    this.logger.Debug().Str("storeType", this.storeType).Msg("starting-gateway-post-0");
    // add storekey to url
    const storeKey = this._url.getParam("storekey");
    this.logger.Debug().Str("storeType", this.storeType).Msg("starting-gateway-post-1");
    if (storeKey !== "insecure") {
      this.logger.Debug().Str("storeType", this.storeType).Msg("starting-gateway-post-2");
      const idx = this._url.getParam("index");
      const storeKeyName = [this.name];
      if (idx) {
        storeKeyName.push(idx);
      }
      storeKeyName.push(this.storeType);
      this.logger.Debug().Str("storeType", this.storeType).Msg("starting-gateway-sk-0");
      let keyName = `@${storeKeyName.join(":")}@`;
      let failIfNotFound = true;
      const kb = await this.keybag();
      this.logger.Debug().Str("storeType", this.storeType).Msg("starting-gateway-sk-1");
      try {
        if (storeKey && storeKey.startsWith("@") && storeKey.endsWith("@")) {
          keyName = storeKey;
          // create if key-ref is given
          this.logger.Debug().Str("storeType", this.storeType).Msg("starting-gateway-sk-1.0a");
          await kb.getNamedKey(keyName);
          this.logger.Debug().Str("storeType", this.storeType).Msg("starting-gateway-sk-1.1");
        } else if (storeKey) {
          this.logger.Debug().Str("storeType", this.storeType).Msg("starting-gateway-sk-1.0b");
          const ret = await kb.getNamedKey(keyName, true);
          this.logger.Debug().Str("storeType", this.storeType).Msg("starting-gateway-sk-1.2");
          if (ret.isErr()) {
            await kb.setNamedKey(keyName, storeKey);
            this.logger.Debug().Str("storeType", this.storeType).Msg("starting-gateway-sk-1.3");
          }
        } else {
          failIfNotFound = false; // create key if not found
        }
      } catch (e) {
        this.logger.Error().Err(e).Msg("error getting key").AsError();
        return Result.Err(e as Error);
      }
      this.logger.Debug().Str("storeType", this.storeType).Msg("starting-gateway-sk-2");
      const ret = await kb.getNamedKey(keyName, failIfNotFound);
      this.logger.Debug().Str("storeType", this.storeType).Msg("starting-gateway-sk-3");
      if (ret.isErr()) {
        return ret as unknown as Result<URI>;
      }
      this._url = this._url.build().setParam("storekey", keyName).URI();
    }
    this.logger.Debug().Str("storeType", this.storeType).Msg("starting-gateway-0");
    const version = guardVersion(this._url);
    if (version.isErr()) {
      this.logger.Error().Result("version", version).Msg("guardVersion");
      await this.close();
      return version;
    }
    if (this.ready) {
      const fn = this.ready.bind(this);
      this.logger.Debug().Str("storeType", this.storeType).Msg("starting-gateway-1");
      const ready = await exception2Result(fn);
      if (ready.isErr()) {
        await this.close();
        return ready as Result<URI>;
      }
    }
    this.logger.Debug().Str("storeType", this.storeType).Msg("starting-gateway-2");
    this._onStarted.forEach((fn) => fn());
    this.logger.Debug().Msg("started");
    return version;
  }
}

export class MetaStoreImpl extends BaseStoreImpl implements MetaStore {
  readonly storeType = "meta";
  readonly subscribers = new Map<string, LoadHandler[]>();

  constructor(name: string, url: URI, opts: StoreOpts) {
    // const my = new URL(url.toString());
    // my.searchParams.set("storekey", 'insecure');
    super(name, url, {
      ...opts,
      logger: ensureLogger(opts.logger, "MetaStoreImpl"),
    });
  }

  onLoad(branch: string, loadHandler: LoadHandler): () => void {
    const subscribers = this.subscribers.get(branch) || [];
    subscribers.push(loadHandler);
    this.subscribers.set(branch, subscribers);
    return () => {
      const subscribers = this.subscribers.get(branch) || [];
      const idx = subscribers.indexOf(loadHandler);
      if (idx > -1) subscribers.splice(idx, 1);
    };
  }

  makeHeader({ cars }: DbMeta): ToString<DbMeta> {
    const toEncode: DbMeta = { cars };
    // if (key) toEncode.key = key;
    return format(toEncode);
  }

  parseHeader(headerData: ToString<DbMeta>): DbMeta {
    const got = parse<DbMeta>(headerData);
    return got;
  }

  async handleSubscribers(dbMetas: DbMeta[], branch: string) {
    try {
      const subscribers = this.subscribers.get(branch) || [];
      this.logger.Debug().Int("subslen", subscribers.length).Msg("marty-before-handle-subscriber");
      await Promise.all(subscribers.map((subscriber) => subscriber(dbMetas)));
    } catch (e) {
      this.logger.Error().Err(e).Msg("handleSubscribers").AsError();
    }
  }

  async handleByteHeads(byteHeads: Uint8Array[], branch = "main") {
    let dbMetas: DbMeta[];
    try {
      dbMetas = this.dbMetasForByteHeads(byteHeads);
    } catch (e) {
      throw this.logger.Error().Err(e).Msg("parseHeader").AsError();
    }
    this.logger.Debug().Msg("marty-handle-byte-heads-before-subs");
    await this.handleSubscribers(dbMetas, branch);
    this.logger.Debug().Msg("marty-handle-byte-heads-after-subs");
    return dbMetas;
  }

  dbMetasForByteHeads(byteHeads: Uint8Array[]) {
    return byteHeads.map((bytes) => {
      const txt = this.textDecoder.decode(bytes);
      this.logger.Debug().Str("txt", txt).Msg("dbMetasForByteHeads");
      return this.parseHeader(txt);
    });
  }

  async load(branch?: string): Promise<DbMeta[] | Falsy> {
    branch = branch || "main";
    this.logger.Debug().Str("branch", branch).Msg("marty-loading");
    const url = await this.gateway.buildUrl(this.url(), branch);
    if (url.isErr()) {
      throw this.logger.Error().Result("buidUrl", url).Str("branch", branch).Msg("got error from gateway.buildUrl").AsError();
    }
    this.logger.Debug().Str("url", url.Ok().toString()).Msg("marty-load-before");
    const bytes = await this.gateway.get(url.Ok());
    this.logger.Debug().Msg("marty-load-after");
    if (bytes.isErr()) {
      this.logger.Debug().Msg("marty-load-bytesiserr");
      if (isNotFoundError(bytes)) {
        this.logger.Debug().Msg("marty-load-notfound");
        return undefined;
      }
      this.logger.Debug().Msg("marty-load-throw");
      throw this.logger.Error().Url(url.Ok()).Result("bytes:", bytes).Msg("gateway get").AsError();
    }
    this.logger.Debug().Msg("marty-load-beforereturn");
    return this.handleByteHeads([bytes.Ok()], branch);
  }

  async save(meta: DbMeta, branch?: string): Promise<Result<void>> {
    branch = branch || "main";
    this.logger.Debug().Str("branch", branch).Any("meta", meta).Msg("saving meta");
    const bytes = this.makeHeader(meta);
    const url = await this.gateway.buildUrl(this.url(), branch);
    if (url.isErr()) {
      throw this.logger.Error().Err(url.Err()).Str("branch", branch).Msg("got error from gateway.buildUrl").AsError();
    }
    const res = await this.gateway.put(url.Ok(), this.textEncoder.encode(bytes));
    if (res.isErr()) {
      throw this.logger.Error().Err(res.Err()).Msg("got error from gateway.put").AsError();
    }
    await this.handleSubscribers([meta], branch);
    return res;
  }

  async close(): Promise<Result<void>> {
    await this.gateway.close(this.url());
    this._onClosed.forEach((fn) => fn());
    return Result.Ok(undefined);
  }
  async destroy(): Promise<Result<void>> {
    return this.gateway.destroy(this.url());
  }
}

export class DataStoreImpl extends BaseStoreImpl implements DataStore {
  readonly storeType = "data";
  // readonly tag: string = "car-base";

  constructor(name: string, url: URI, opts: StoreOpts) {
    super(name, url, {
      ...opts,
      logger: ensureLogger(opts.logger, "DataStoreImpl"),
    });
  }

  async load(cid: AnyLink): Promise<AnyBlock> {
    this.logger.Debug().Any("cid", cid).Msg("loading");
    const url = await this.gateway.buildUrl(this.url(), cid.toString());
    if (url.isErr()) {
      throw this.logger.Error().Err(url.Err()).Str("cid", cid.toString()).Msg("got error from gateway.buildUrl").AsError();
    }
    const res = await this.gateway.get(url.Ok());
    if (res.isErr()) {
      throw res.Err();
    }
    return { cid, bytes: res.Ok() };
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  async save(car: AnyBlock, opts?: DataSaveOpts): Promise</*AnyLink | */ void> {
    this.logger.Debug().Any("cid", car.cid.toString()).Msg("saving");
    const url = await this.gateway.buildUrl(this.url(), car.cid.toString());
    if (url.isErr()) {
      throw this.logger.Error().Err(url.Err()).Ref("cid", car.cid).Msg("got error from gateway.buildUrl").AsError();
    }
    const res = await this.gateway.put(url.Ok(), car.bytes);
    if (res.isErr()) {
      throw this.logger.Error().Err(res.Err()).Msg("got error from gateway.put").AsError();
    }
    return res.Ok();
  }
  async remove(cid: AnyLink): Promise<Result<void>> {
    const url = await this.gateway.buildUrl(this.url(), cid.toString());
    if (url.isErr()) {
      return url;
    }
    return this.gateway.delete(url.Ok());
  }
  async close(): Promise<Result<void>> {
    await this.gateway.close(this.url());
    this._onClosed.forEach((fn) => fn());
    return Result.Ok(undefined);
  }
  destroy(): Promise<Result<void>> {
    return this.gateway.destroy(this.url());
  }
}

export class WALStoreImpl extends BaseStoreImpl implements WALStore {
  readonly storeType = "wal";
  // readonly tag: string = "rwal-base";

  readonly loader: Loadable;

  readonly _ready = new ResolveOnce<void>();

  walState: WALState = { operations: [], noLoaderOps: [], fileOperations: [] };
  readonly processing: Promise<void> | undefined = undefined;
  readonly processQueue: CommitQueue<void> = new CommitQueue<void>();

  constructor(loader: Loadable, url: URI, opts: StoreOpts) {
    // const my = new URL(url.toString());
    // my.searchParams.set("storekey", 'insecure');
    super(loader.name, url, {
      ...opts,
      logger: ensureLogger(opts.logger, "WALStoreImpl"),
    });
    this.loader = loader;
  }

  ready = async () => {
    return this._ready.once(async () => {
      const walState = await this.load().catch((e) => {
        this.logger.Error().Any("error", e).Msg("error loading wal");
        return undefined;
      });
      if (!walState) {
        this.walState.operations = [];
        this.walState.fileOperations = [];
      } else {
        this.walState.operations = walState.operations || [];
        this.walState.fileOperations = walState.fileOperations || [];
      }
    });
  };

  async enqueue(dbMeta: DbMeta, opts: CommitOpts) {
    await this.ready();
    if (opts.noLoader) {
      this.walState.noLoaderOps.push(dbMeta);
    } else {
      this.walState.operations.push(dbMeta);
    }
    await this.save(this.walState);
    void this.process();
  }

  async enqueueFile(fileCid: AnyLink, publicFile = false) {
    await this.ready();
    this.walState.fileOperations.push({ cid: fileCid, public: publicFile });
    // await this.save(this.walState)
  }

  async process() {
    await this.ready();
    if (!this.loader.remoteCarStore) return;
    await this.processQueue.enqueue(async () => {
      try {
        await this._doProcess();
      } catch (e) {
        this.logger.Error().Any("error", e).Msg("error processing wal");
      }
      if (this.walState.operations.length || this.walState.fileOperations.length || this.walState.noLoaderOps.length) {
        setTimeout(() => void this.process(), 0);
      }
    });
  }

  async _doProcess() {
    this.logger.Debug().Msg("do-process-0");
    if (!this.loader.remoteCarStore) return;
    const rmlp = (async () => {
      const operations = [...this.walState.operations];
      const fileOperations = [...this.walState.fileOperations];
      const uploads: Promise<void>[] = [];
      const noLoaderOps = [...this.walState.noLoaderOps];
      const limit = pLimit(5);

      if (operations.length + fileOperations.length + noLoaderOps.length === 0) return;

      this.logger.Debug().Msg("rmlp-1");

      for (const dbMeta of noLoaderOps) {
        const uploadP = limit(async () => {
          for (const cid of dbMeta.cars) {
            const car = await (await this.loader.carStore()).load(cid);
            if (!car) {
              if (carLogIncludesGroup(this.loader.carLog, dbMeta.cars))
                throw this.logger.Error().Ref("cid", cid).Msg("missing local car").AsError();
            } else {
              await throwFalsy(this.loader.remoteCarStore).save(car);
            }
            this.walState.noLoaderOps = this.walState.noLoaderOps.filter((op) => op !== dbMeta);
          }
        });
        uploads.push(uploadP);
      }

      this.logger.Debug().Msg("rmlp-2");

      for (const dbMeta of operations) {
        const uploadP = limit(async () => {
          for (const cid of dbMeta.cars) {
            const car = await (await this.loader.carStore()).load(cid).catch(() => null);
            if (!car) {
              if (carLogIncludesGroup(this.loader.carLog, dbMeta.cars))
                throw this.logger.Error().Ref("cid", cid).Msg(`missing local car`).AsError();
            } else {
              await throwFalsy(this.loader.remoteCarStore).save(car);
            }
          }
          this.walState.operations = this.walState.operations.filter((op) => op !== dbMeta);
        });
        uploads.push(uploadP);
      }

      this.logger.Debug().Msg("rmlp-3");

      if (fileOperations.length) {
        const dbLoader = this.loader;
        for (const { cid: fileCid, public: publicFile } of fileOperations) {
          const uploadP = limit(async () => {
            const fileBlock = await (await dbLoader.fileStore()).load(fileCid); // .catch(() => false)
            await dbLoader.remoteFileStore?.save(fileBlock, { public: publicFile });
            this.walState.fileOperations = this.walState.fileOperations.filter((op) => op.cid !== fileCid);
          });
          uploads.push(uploadP);
        }
      }

      this.logger.Debug().Msg("rmlp-4");

      try {
        const res = await Promise.allSettled(uploads);
        const errors = res.filter((r) => r.status === "rejected") as PromiseRejectedResult[];
        if (errors.length) {
          throw this.logger.Error().Any("errors", errors).Msg("error uploading").AsError();
        }
        if (operations.length) {
          const lastOp = operations[operations.length - 1];
          // console.log('saving remote meta', lastOp.car.toString())
          await this.loader.remoteMetaStore?.save(lastOp).catch((e: Error) => {
            this.walState.operations.push(lastOp);
            throw this.logger.Error().Any("error", e).Msg("error saving remote meta").AsError();
          });
        }
      } finally {
        await this.save(this.walState);
      }

      this.logger.Debug().Msg("rmlp-5");
    })();
    // this.loader.remoteMetaLoading = rmlp;
    this.logger.Debug().Msg("do-process-2");
    await rmlp;
  }

  async load(): Promise<WALState | Falsy> {
    this.logger.Debug().Msg("loading");
    const filepath = await this.gateway.buildUrl(this.url(), "main");
    if (filepath.isErr()) {
      throw this.logger.Error().Err(filepath.Err()).Url(this.url()).Msg("error building url").AsError();
    }
    const bytes = await this.gateway.get(filepath.Ok());
    if (bytes.isErr()) {
      if (isNotFoundError(bytes)) {
        return undefined;
      }
      throw this.logger.Error().Err(bytes.Err()).Msg("error get").AsError();
    }
    try {
      return bytes && parse<WALState>(this.textDecoder.decode(bytes.Ok()));
    } catch (e) {
      throw this.logger.Error().Err(e).Msg("error parse").AsError();
    }
  }

  async save(state: WALState) {
    const filepath = await this.gateway.buildUrl(this.url(), "main");
    if (filepath.isErr()) {
      throw this.logger.Error().Err(filepath.Err()).Url(this.url()).Msg("error building url").AsError();
    }
    let encoded: ToString<WALState>;
    try {
      encoded = format(state);
    } catch (e) {
      throw this.logger.Error().Err(e).Any("state", state).Msg("error format").AsError();
    }
    const res = await this.gateway.put(filepath.Ok(), this.textEncoder.encode(encoded));
    if (res.isErr()) {
      throw this.logger.Error().Err(res.Err()).Str("filePath", filepath.Ok().toString()).Msg("error saving").AsError();
    }
  }

  async close() {
    await this.gateway.close(this.url());
    this._onClosed.forEach((fn) => fn());
    return Result.Ok(undefined);
  }

  destroy() {
    return this.gateway.destroy(this.url());
  }
}
