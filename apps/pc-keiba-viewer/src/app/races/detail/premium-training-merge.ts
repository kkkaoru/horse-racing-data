// Run with bun (bunx vitest).
// Premium training review merging logic.
//
// `mergePremiumTrainingReviews` attaches each `PremiumTrainingReview` to a
// displayed training row by horse number and (normalized) training date.
//
// Lookup order per row:
//   1. exact horse + date match (D1 premium feed already aligned by date)
//   2. dateless horse match (netkeiba fallback parser yields dateless entries)
//   3. latest-row fallback - if a dated review never matches any displayed
//      `chokyoNengappi`, attach the most recent dated review to the row whose
//      `chokyoNengappi` is the latest for that horse. Single review per
//      horse, attached to a single row.
//
// The latest-row fallback fixes a viewer regression where premium D1 reviews
// (e.g. `trainingDate=2026/06/24`) would never merge because the displayed
// `chokyoNengappi` values predate the review date.

import type { Training } from "../../../lib/race-types";

export interface PremiumTrainingReview {
  commentText: string | null;
  evaluationGrade: string | null;
  evaluationText: string | null;
  horseNumber: string;
  riderName?: string | null;
  trainingDate: string;
}

interface LookupReviewInput {
  horsesWithExactMatch: Set<string>;
  latestDateByHorse: Map<string, string>;
  reviewByHorse: Map<string, PremiumTrainingReview>;
  reviewByHorseAndDate: Map<string, PremiumTrainingReview>;
  reviewByHorseLatest: Map<string, PremiumTrainingReview>;
  training: Training;
}

const NON_DIGIT_PATTERN = /[^\d]/gu;

const normalizeTrainingDate = (value: string): string => value.replace(NON_DIGIT_PATTERN, "");

const computeHorseNumber = (umaban: string | null): string =>
  umaban ? String(Number(umaban)) : "";

const buildReviewByHorseAndDate = (
  reviews: readonly PremiumTrainingReview[],
): Map<string, PremiumTrainingReview> =>
  new Map(
    reviews
      .filter((review) => normalizeTrainingDate(review.trainingDate))
      .map((review) => [
        `${review.horseNumber}-${normalizeTrainingDate(review.trainingDate)}`,
        review,
      ]),
  );

const buildReviewByHorse = (
  reviews: readonly PremiumTrainingReview[],
): Map<string, PremiumTrainingReview> =>
  new Map(
    reviews
      .filter((review) => !normalizeTrainingDate(review.trainingDate))
      .map((review) => [review.horseNumber, review]),
  );

const reduceLatestReview = (
  acc: Map<string, PremiumTrainingReview>,
  review: PremiumTrainingReview,
): Map<string, PremiumTrainingReview> => {
  const existing = acc.get(review.horseNumber);
  if (!existing) {
    acc.set(review.horseNumber, review);
    return acc;
  }
  const reviewDate = normalizeTrainingDate(review.trainingDate);
  const existingDate = normalizeTrainingDate(existing.trainingDate);
  if (reviewDate > existingDate) {
    acc.set(review.horseNumber, review);
  }
  return acc;
};

const buildReviewByHorseLatest = (
  reviews: readonly PremiumTrainingReview[],
): Map<string, PremiumTrainingReview> =>
  reviews
    .filter((review) => normalizeTrainingDate(review.trainingDate))
    .reduce(reduceLatestReview, new Map<string, PremiumTrainingReview>());

const reduceLatestDate = (acc: Map<string, string>, training: Training): Map<string, string> => {
  const horseNumber = computeHorseNumber(training.umaban);
  if (!horseNumber) {
    return acc;
  }
  const date = normalizeTrainingDate(training.chokyoNengappi);
  if (!date) {
    return acc;
  }
  const existing = acc.get(horseNumber);
  if (!existing || date > existing) {
    acc.set(horseNumber, date);
  }
  return acc;
};

const buildLatestDateByHorse = (trainings: readonly Training[]): Map<string, string> =>
  trainings.reduce(reduceLatestDate, new Map<string, string>());

const buildHorsesWithExactMatch = (
  trainings: readonly Training[],
  reviewByHorseAndDate: Map<string, PremiumTrainingReview>,
): Set<string> =>
  new Set(
    trainings
      .map((training) => {
        const horseNumber = computeHorseNumber(training.umaban);
        if (!horseNumber) {
          return null;
        }
        const date = normalizeTrainingDate(training.chokyoNengappi);
        return reviewByHorseAndDate.has(`${horseNumber}-${date}`) ? horseNumber : null;
      })
      .filter((value): value is string => value !== null),
  );

const lookupReviewForTraining = ({
  horsesWithExactMatch,
  latestDateByHorse,
  reviewByHorse,
  reviewByHorseAndDate,
  reviewByHorseLatest,
  training,
}: LookupReviewInput): PremiumTrainingReview | undefined => {
  const horseNumber = computeHorseNumber(training.umaban);
  if (!horseNumber) {
    return undefined;
  }
  const trainingDate = normalizeTrainingDate(training.chokyoNengappi);
  const exactMatch = reviewByHorseAndDate.get(`${horseNumber}-${trainingDate}`);
  if (exactMatch) {
    return exactMatch;
  }
  const datelessMatch = reviewByHorse.get(horseNumber);
  if (datelessMatch) {
    return datelessMatch;
  }
  if (horsesWithExactMatch.has(horseNumber)) {
    return undefined;
  }
  const latestDate = latestDateByHorse.get(horseNumber);
  if (!latestDate || latestDate !== trainingDate) {
    return undefined;
  }
  return reviewByHorseLatest.get(horseNumber);
};

const applyReviewToTraining = (training: Training, review: PremiumTrainingReview): Training => ({
  ...training,
  premiumCommentText: review.commentText,
  premiumEvaluationGrade: review.evaluationGrade,
  premiumEvaluationText: review.evaluationText,
  trainingRiderName: review.riderName ?? training.trainingRiderName,
});

export const mergePremiumTrainingReviews = (
  trainings: Training[],
  reviews: readonly PremiumTrainingReview[],
): Training[] => {
  if (reviews.length === 0) {
    return trainings;
  }
  const reviewByHorseAndDate = buildReviewByHorseAndDate(reviews);
  const reviewByHorse = buildReviewByHorse(reviews);
  const reviewByHorseLatest = buildReviewByHorseLatest(reviews);
  const latestDateByHorse = buildLatestDateByHorse(trainings);
  const horsesWithExactMatch = buildHorsesWithExactMatch(trainings, reviewByHorseAndDate);
  return trainings.map((training) => {
    const review = lookupReviewForTraining({
      horsesWithExactMatch,
      latestDateByHorse,
      reviewByHorse,
      reviewByHorseAndDate,
      reviewByHorseLatest,
      training,
    });
    return review ? applyReviewToTraining(training, review) : training;
  });
};
