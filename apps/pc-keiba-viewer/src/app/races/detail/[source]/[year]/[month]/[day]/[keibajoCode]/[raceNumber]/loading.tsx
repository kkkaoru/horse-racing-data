const LoadingBlock = () => <span />;

export default function Loading() {
  return (
    <section className="page-shell detail-page-loading" aria-busy="true">
      <div className="race-global-summary skeleton-summary">
        <div>
          <span />
          <span />
          <span />
          <span />
        </div>
      </div>
      <div className="breadcrumbs skeleton-line">
        <span />
        <span />
        <span />
      </div>
      <div className="detail-hero skeleton-hero">
        <div>
          <p className="skeleton-text short" />
          <div className="skeleton-text title" />
          <p className="skeleton-text medium" />
        </div>
        <div className="race-badge skeleton-badge" />
      </div>
      <section className="detail-grid skeleton-grid" aria-label="race details loading">
        {Array.from({ length: 10 }, (_, index) => (
          <div className="detail-cell" key={index}>
            <LoadingBlock />
            <LoadingBlock />
          </div>
        ))}
      </section>
      <section className="detail-loading-section">
        <div className="section-heading compact">
          <h2>出走馬</h2>
          <span>読み込み中</span>
        </div>
        <div className="detail-section-skeleton">
          <LoadingBlock />
          <LoadingBlock />
          <LoadingBlock />
          <LoadingBlock />
        </div>
      </section>
    </section>
  );
}
