const LoadingBlock = () => <span />;

const LoadingSection = ({
  compact = false,
  rows = 4,
  title,
}: {
  compact?: boolean;
  rows?: number;
  title: string;
}) => (
  <section className="detail-loading-section">
    <div className="section-heading compact">
      <h2>{title}</h2>
      <span>読み込み中</span>
    </div>
    <div className={compact ? "detail-section-skeleton compact" : "detail-section-skeleton"}>
      {Array.from({ length: rows }, (_, index) => (
        <LoadingBlock key={index} />
      ))}
    </div>
  </section>
);

export default function Loading() {
  return (
    <section className="page-shell detail-page-loading" aria-busy="true">
      <div className="race-global-summary skeleton-summary">
        <div>
          <span />
          <span />
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
        {Array.from({ length: 12 }, (_, index) => (
          <div className="detail-cell" key={index}>
            <LoadingBlock />
            <LoadingBlock />
          </div>
        ))}
      </section>
      <LoadingSection compact rows={3} title="馬場状態" />
      <LoadingSection rows={5} title="パドック速報" />
      <LoadingSection rows={6} title="パドック" />
      <LoadingSection rows={5} title="レース傾向" />
      <LoadingSection compact rows={4} title="コース情報" />
      <LoadingSection rows={6} title="出走馬" />
      <LoadingSection rows={4} title="着順予測" />
      <LoadingSection rows={4} title="リアルタイムデータ" />
      <LoadingSection rows={5} title="タイム・相関・血統・同条件スコア" />
      <LoadingSection rows={5} title="競走成績" />
    </section>
  );
}
