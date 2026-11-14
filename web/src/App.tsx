import { Link, Route, Routes } from "react-router-dom";
import { JobListPage } from "./pages/JobListPage";
import { JobCreatePage } from "./pages/JobCreatePage";
import { JobDetailPage } from "./pages/JobDetailPage";
import { CampaignListPage } from "./pages/CampaignListPage";
import { CampaignCreatePage } from "./pages/CampaignCreatePage";
import { CampaignDetailPage } from "./pages/CampaignDetailPage";

export function App() {
  return (
    <div className="layout">
      <header className="header">
        <h1>Marketing scheduler</h1>
        <p className="banner">
          Mock sender only — <strong>succeeded</strong> means the test sender finished, not real delivery.
        </p>
        <nav>
          <Link to="/jobs">Single jobs</Link>
          <Link to="/campaigns">Campaigns</Link>
        </nav>
      </header>
      <main>
        <Routes>
          <Route path="/" element={<JobListPage />} />
          <Route path="/jobs" element={<JobListPage />} />
          <Route path="/jobs/new" element={<JobCreatePage />} />
          <Route path="/jobs/:id" element={<JobDetailPage />} />
          <Route path="/campaigns" element={<CampaignListPage />} />
          <Route path="/campaigns/new" element={<CampaignCreatePage />} />
          <Route path="/campaigns/:id" element={<CampaignDetailPage />} />
        </Routes>
      </main>
    </div>
  );
}