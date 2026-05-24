// Route-level loading UI for the paddock edit page. Without this file the
// router falls back to the parent race-detail loading skeleton, which
// renders ~10 section placeholders that look nothing like the paddock
// board. This loader mirrors the actual paddock layout so the transition
// from the race-detail page reads as a paddock load, not a fresh detail
// load.

const RUNNER_SKELETON_COUNT = 8;
const SCORE_CONTROL_SKELETON_COUNT = 3;

const PaddockRunnerSkeleton = () => (
  <article className="paddock-horse-row paddock-horse-row-skeleton" aria-hidden="true">
    <header className="paddock-horse-summary">
      <dl className="paddock-horse-ids">
        <div>
          <dt>枠番</dt>
          <dd>
            <span className="skeleton-text short" />
          </dd>
        </div>
        <div>
          <dt>馬番</dt>
          <dd>
            <span className="skeleton-text short" />
          </dd>
        </div>
      </dl>
      <div className="paddock-horse-name-block">
        <span className="skeleton-text medium" />
        <span className="skeleton-text short" />
      </div>
      <dl className="paddock-horse-race-facts">
        {Array.from({ length: 5 }, (_, index) => (
          <div key={index}>
            <dt>
              <span className="skeleton-text short" />
            </dt>
            <dd>
              <span className="skeleton-text short" />
            </dd>
          </div>
        ))}
      </dl>
    </header>
    <section className="paddock-recent-results paddock-recent-results-loading">
      <h3>近走</h3>
      <ol>
        {Array.from({ length: 3 }, (_, index) => (
          <li className="paddock-recent-skeleton-item" key={index}>
            <span className="paddock-recent-finish skeleton-text short" />
            <span className="paddock-recent-race">
              <strong className="skeleton-text medium" />
              <small className="skeleton-text short" />
              <small className="skeleton-text short" />
            </span>
          </li>
        ))}
      </ol>
    </section>
    <ul className="paddock-score-controls" aria-hidden="true">
      {Array.from({ length: SCORE_CONTROL_SKELETON_COUNT }, (_, index) => (
        <li className="paddock-score-control" key={index}>
          <span className="skeleton-text short" />
          <span className="skeleton-text short" />
        </li>
      ))}
    </ul>
  </article>
);

export default function Loading() {
  return (
    <main className="page-shell paddock-edit-loading" aria-busy="true">
      <section className="race-global-summary skeleton-summary" aria-hidden="true">
        <div>
          <span />
          <span />
          <span />
          <span />
          <span />
          <span />
        </div>
      </section>
      <nav className="breadcrumbs skeleton-line" aria-hidden="true">
        <span />
        <span />
        <span />
        <span />
      </nav>
      <header className="page-title-row paddock-edit-title-row skeleton-hero" aria-hidden="true">
        <div>
          <p className="skeleton-text short" />
          <div className="skeleton-text title" />
          <p className="skeleton-text medium" />
        </div>
        <span className="paddock-edit-link skeleton-text short" />
      </header>
      <section className="paddock-section paddock-section-edit" aria-hidden="true">
        <header className="section-heading compact">
          <h2>パドック</h2>
          <span>読み込み中</span>
        </header>
        <div className="paddock-board">
          {Array.from({ length: RUNNER_SKELETON_COUNT }, (_, index) => (
            <PaddockRunnerSkeleton key={index} />
          ))}
        </div>
      </section>
    </main>
  );
}
