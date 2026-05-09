import "server-only";
import { pgTable, varchar } from "drizzle-orm/pg-core";

const raceColumns = {
  kaisaiNen: varchar("kaisai_nen", { length: 4 }).notNull(),
  kaisaiTsukihi: varchar("kaisai_tsukihi", { length: 4 }).notNull(),
  keibajoCode: varchar("keibajo_code", { length: 2 }).notNull(),
  raceBango: varchar("race_bango", { length: 2 }).notNull(),
  kyosomeiHondai: varchar("kyosomei_hondai", { length: 60 }),
  kyosomeiFukudai: varchar("kyosomei_fukudai", { length: 60 }),
  kyosomeiKakkonai: varchar("kyosomei_kakkonai", { length: 60 }),
  gradeCode: varchar("grade_code", { length: 1 }),
  kyosoShubetsuCode: varchar("kyoso_shubetsu_code", { length: 2 }),
  kyosoKigoCode: varchar("kyoso_kigo_code", { length: 3 }),
  juryoShubetsuCode: varchar("juryo_shubetsu_code", { length: 1 }),
  kyosoJokenCode: varchar("kyoso_joken_code", { length: 3 }),
  kyosoJokenMeisho: varchar("kyoso_joken_meisho", { length: 60 }),
  kyori: varchar("kyori", { length: 4 }),
  trackCode: varchar("track_code", { length: 2 }),
  hassoJikoku: varchar("hasso_jikoku", { length: 4 }),
  torokuTosu: varchar("toroku_tosu", { length: 2 }),
  shussoTosu: varchar("shusso_tosu", { length: 2 }),
  tenkoCode: varchar("tenko_code", { length: 1 }),
  babajotaiCodeShiba: varchar("babajotai_code_shiba", { length: 1 }),
  babajotaiCodeDirt: varchar("babajotai_code_dirt", { length: 1 }),
};

const runnerColumns = {
  kaisaiNen: varchar("kaisai_nen", { length: 4 }).notNull(),
  kaisaiTsukihi: varchar("kaisai_tsukihi", { length: 4 }).notNull(),
  keibajoCode: varchar("keibajo_code", { length: 2 }).notNull(),
  raceBango: varchar("race_bango", { length: 2 }).notNull(),
  wakuban: varchar("wakuban", { length: 1 }),
  umaban: varchar("umaban", { length: 2 }),
  kettoTorokuBango: varchar("ketto_toroku_bango", { length: 10 }),
  bamei: varchar("bamei", { length: 36 }),
  seibetsuCode: varchar("seibetsu_code", { length: 1 }),
  barei: varchar("barei", { length: 2 }),
  futanJuryo: varchar("futan_juryo", { length: 3 }),
  kishumeiRyakusho: varchar("kishumei_ryakusho", { length: 8 }),
  chokyoshimeiRyakusho: varchar("chokyoshimei_ryakusho", { length: 8 }),
  banushimei: varchar("banushimei", { length: 64 }),
  bataiju: varchar("bataiju", { length: 3 }),
  zogenFugo: varchar("zogen_fugo", { length: 1 }),
  zogenSa: varchar("zogen_sa", { length: 3 }),
  kakuteiChakujun: varchar("kakutei_chakujun", { length: 2 }),
  tanshoOdds: varchar("tansho_odds", { length: 4 }),
  tanshoNinkijun: varchar("tansho_ninkijun", { length: 2 }),
  sohaTime: varchar("soha_time", { length: 4 }),
  timeSa: varchar("time_sa", { length: 4 }),
  kohan3f: varchar("kohan_3f", { length: 3 }),
};

export const jvdCs = pgTable("jvd_cs", {
  keibajoCode: varchar("keibajo_code", { length: 2 }).notNull(),
  kyori: varchar("kyori", { length: 4 }).notNull(),
  trackCode: varchar("track_code", { length: 2 }).notNull(),
  courseKaishuNengappi: varchar("course_kaishu_nengappi", { length: 8 }).notNull(),
  courseSetsumei: varchar("course_setsumei", { length: 6800 }),
});

export const jvdRa = pgTable("jvd_ra", raceColumns);
export const nvdRa = pgTable("nvd_ra", raceColumns);
export const jvdSe = pgTable("jvd_se", runnerColumns);
export const nvdSe = pgTable("nvd_se", runnerColumns);
