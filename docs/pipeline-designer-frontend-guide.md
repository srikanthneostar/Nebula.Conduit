# Pipeline Designer Frontend — Development Guide

> This document is a complete reference for building the Pipeline Designer UI in the Nebula.ng React project.
> It integrates with the Nebula.Conduit backend REST API and follows the existing project theming (glass morphism),
> component library, and routing patterns already established in the codebase.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [File Structure](#2-file-structure)
3. [TypeScript Interfaces](#3-typescript-interfaces)
4. [REST API Service Layer](#4-rest-api-service-layer)
5. [Route Configuration](#5-route-configuration)
6. [Pipeline List Page](#6-pipeline-list-page)
7. [Pipeline Designer Page (React Flow)](#7-pipeline-designer-page-react-flow)
8. [Component Palette (Node Palette)](#8-component-palette-node-palette)
9. [Custom React Flow Nodes](#9-custom-react-flow-nodes)
10. [Component Configuration Panel](#10-component-configuration-panel)
11. [Connection Validation Rules](#11-connection-validation-rules)
12. [Pipeline Execution Controls](#12-pipeline-execution-controls)
13. [React Flow Theming](#13-react-flow-theming)
14. [State Management](#14-state-management)
15. [Backend API Reference](#15-backend-api-reference)
16. [Component Type Reference](#16-component-type-reference)
17. [Sample Pipeline JSON](#17-sample-pipeline-json)
18. [Pipeline Import & Export](#18-pipeline-import--export)

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│  Nebula.ng React App                                    │
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │ Pipeline     │  │ Pipeline     │  │ Execution    │  │
│  │ List Page    │──│ Designer     │  │ History      │  │
│  │              │  │ (React Flow) │  │ Panel        │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
│         │                  │                  │          │
│         └──────────┬───────┴──────────────────┘          │
│                    │                                     │
│         ┌──────────▼──────────┐                          │
│         │  PipelineService    │  (REST fetch client)     │
│         └──────────┬──────────┘                          │
└────────────────────┼────────────────────────────────────┘
                     │ HTTPS + JWT Bearer
                     ▼
┌────────────────────────────────────────────────────────┐
│  Nebula.Conduit Backend (Go)                           │
│  https://localhost:8080/api/v1/pipelines/*              │
└────────────────────────────────────────────────────────┘
```

The backend is a REST API (not GraphQL). The existing project uses Apollo/GraphQL for other modules,
but pipelines use plain `fetch()` calls via a dedicated service class. The auth token is stored in
`sessionStorage` under key `authToken` and accessed via `secureStorage.get('AUTH_TOKEN')`.

---

## 2. File Structure

Create these files inside the Nebula.ng project:

```
src/
├── types/
│   └── pipeline.ts                          # All TypeScript interfaces
├── services/
│   └── pipelineService.ts                   # REST API client for pipeline endpoints
├── pages/
│   └── pipelines/
│       ├── PipelineListPage.tsx              # List page (follows IdentityNewListPage pattern)
│       └── PipelineDesignerPage.tsx          # Designer page (React Flow canvas + config panel)
├── components/
│   └── pipeline/
│       ├── PipelineDesigner.tsx              # React Flow canvas wrapper
│       ├── PipelineNodePalette.tsx           # Draggable component palette sidebar
│       ├── PipelineConfigPanel.tsx           # Right-side config panel for selected node
│       ├── PipelineExecutionPanel.tsx        # Execution controls + history
│       ├── PipelineToolbar.tsx               # Top toolbar (save, run, validate, etc.)
│       └── nodes/
│           ├── SourceNode.tsx                # Custom node for source components
│           ├── ProcessorNode.tsx             # Custom node for processor components
│           └── SinkNode.tsx                  # Custom node for sink components
└── hooks/
    └── usePipelineApi.ts                    # React hooks wrapping PipelineService
```

---

## 3. TypeScript Interfaces

Create `src/types/pipeline.ts`. These mirror the Go backend models exactly.

```typescript
// src/types/pipeline.ts

// ─── Enums ───────────────────────────────────────────────

export type ExecutionMode = 'scheduled' | 'continuous';
export type PipelineStatus = 'active' | 'inactive';
export type InstanceStatus = 'running' | 'completed' | 'failed' | 'stopped';

export type ComponentType =
  // Sources
  | 'http_get'
  | 'sql_query'
  | 'csv_reader'
  | 'kafka_consumer'
  | 'rabbitmq_consumer'
  | 'hl7_reader'
  | 'tcp_read'
  // Processors
  | 'python_code_block'
  | 'log'
  | 'attribute_update'
  // Sinks
  | 'http_post'
  | 'kafka_producer'
  | 'rabbitmq_producer'
  | 'tcp_write'
  | 'log_sink';

export type ComponentCategory = 'source' | 'processor' | 'sink';

// ─── Component Config ────────────────────────────────────

export interface ComponentConfig {
  id: string;
  type: ComponentType;
  parameters: Record<string, any>;
  retry_count: number;
  retry_delay: number;        // nanoseconds (Go time.Duration)
  continue_on_error: boolean;
  timeout: number;            // nanoseconds
}

export interface Connection {
  source_component_id: string;
  target_component_id: string;
}

// ─── Pipeline Definition ─────────────────────────────────

export interface PipelineDefinition {
  id: string;
  name: string;
  description: string;
  execution_mode: ExecutionMode;
  cron_expression?: string;
  status: PipelineStatus;
  components: ComponentConfig[];
  connections: Connection[];
  created_at: string;
  updated_at: string;
}

// ─── API Request/Response Types ──────────────────────────

export interface CreatePipelineRequest {
  name: string;
  description: string;
  execution_mode: ExecutionMode;
  cron_expression?: string;
  status: PipelineStatus;
  components: ComponentConfig[];
  connections: Connection[];
}

export interface UpdatePipelineRequest {
  name?: string;
  description?: string;
  execution_mode?: ExecutionMode;
  cron_expression?: string;
  status?: PipelineStatus;
  components?: ComponentConfig[];
  connections?: Connection[];
}

export interface ListPipelinesResponse {
  pipelines: PipelineDefinition[];
  total: number;
}

export interface TriggerPipelineResponse {
  instance_id: string;
  pipeline_id: string;
  status: string;
  message: string;
}

export interface StopInstanceResponse {
  instance_id: string;
  status: string;
  message: string;
}

export interface InstanceStatusResponse {
  instance_id: string;
  pipeline_id: string;
  status: InstanceStatus;
  started_at: string;
  components: ComponentStatusInfo[];
}

export interface ComponentStatusInfo {
  component_id: string;
  status: InstanceStatus;
  duration_ms: number;
}

export interface ExecutionRecord {
  id: string;
  pipeline_id: string;
  status: InstanceStatus;
  started_at: string;
  ended_at: string | null;
  error_message: string | null;
  component_results: ComponentExecutionRecord[];
}

export interface ComponentExecutionRecord {
  id: string;
  pipeline_execution_id: string;
  component_id: string;
  status: InstanceStatus;
  started_at: string;
  ended_at: string | null;
  output_data_size: number;
  error_message: string | null;
}

export interface ExecutionHistoryResponse {
  executions: ExecutionRecord[];
  total: number;
  page: number;
  page_size: number;
}

export interface ErrorResponse {
  error: string;
  code?: string;
  details?: Record<string, any>;
}

// ─── Helper: classify component type ─────────────────────

export const SOURCE_TYPES: ComponentType[] = [
  'http_get', 'sql_query', 'csv_reader', 'kafka_consumer',
  'rabbitmq_consumer', 'hl7_reader', 'tcp_read',
];

export const PROCESSOR_TYPES: ComponentType[] = [
  'python_code_block', 'log', 'attribute_update',
];

export const SINK_TYPES: ComponentType[] = [
  'http_post', 'kafka_producer', 'rabbitmq_producer', 'tcp_write', 'log_sink',
];

export function getComponentCategory(type: ComponentType): ComponentCategory {
  if (SOURCE_TYPES.includes(type)) return 'source';
  if (PROCESSOR_TYPES.includes(type)) return 'processor';
  return 'sink';
}
```

---

## 4. REST API Service Layer

The pipeline backend is REST, not GraphQL. Create a dedicated service using `fetch()`.
The auth token is retrieved from `secureStorage.get('AUTH_TOKEN')` (stored in sessionStorage under key `authToken`).

The base URL comes from `common_settings.json` (the project's central config file for host URLs),
under the key `CONDUIT_API_URL`. This follows the same pattern used by `apolloClient.ts` (`API_URL`),
`CameraService.ts`, `VertexAIService.ts`, and `NotificationService.ts`.

### Step 0: Add CONDUIT_API_URL to common_settings.json

Add the Conduit backend URL to the project root `common_settings.json`:

```json
{
  "API_URL": "https://192.168.1.230/api",
  "SELF_API_URL": "/api",
  "DEPLOYMENT_ONPRIM": false,
  "VERTEXT_API_URL": "https://192.168.1.34:8000",
  "CONDUIT_API_URL": "https://localhost:8080",
  "PRODUCTION": false
}
```

### Service Implementation

```typescript
// src/services/pipelineService.ts

import config from '../../common_settings.json';
import { secureStorage } from '@/utils/secureStorage';
import {
  CreatePipelineRequest,
  UpdatePipelineRequest,
  PipelineDefinition,
  ListPipelinesResponse,
  TriggerPipelineResponse,
  StopInstanceResponse,
  InstanceStatusResponse,
  ExecutionHistoryResponse,
  ExecutionRecord,
  ExportPipelinesResponse,
  ExportedPipeline,
  ImportPipelinesResponse,
  ErrorResponse,
} from '@/types/pipeline';

const BASE_URL = config.CONDUIT_API_URL;

class PipelineServiceError extends Error {
  code?: string;
  details?: Record<string, any>;

  constructor(message: string, code?: string, details?: Record<string, any>) {
    super(message);
    this.name = 'PipelineServiceError';
    this.code = code;
    this.details = details;
  }
}

async function request<T>(path: string, options: RequestInit = {}): Promise<T> {
  const token = secureStorage.get('AUTH_TOKEN');

  const res = await fetch(`${BASE_URL}${path}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...options.headers,
    },
  });

  if (!res.ok) {
    let errorBody: ErrorResponse = { error: res.statusText };
    try {
      errorBody = await res.json();
    } catch { /* ignore parse errors */ }
    throw new PipelineServiceError(errorBody.error, errorBody.code, errorBody.details);
  }

  return res.json();
}

export const pipelineService = {
  // ─── Pipeline CRUD ──────────────────────────────────

  async list(status?: string): Promise<ListPipelinesResponse> {
    const query = status ? `?status=${status}` : '';
    return request<ListPipelinesResponse>(`/api/v1/pipelines${query}`);
  },

  async get(id: string): Promise<PipelineDefinition> {
    return request<PipelineDefinition>(`/api/v1/pipelines/${id}`);
  },

  async create(pipeline: CreatePipelineRequest): Promise<PipelineDefinition> {
    return request<PipelineDefinition>('/api/v1/pipelines', {
      method: 'POST',
      body: JSON.stringify(pipeline),
    });
  },

  async update(id: string, pipeline: UpdatePipelineRequest): Promise<PipelineDefinition> {
    return request<PipelineDefinition>(`/api/v1/pipelines/${id}`, {
      method: 'PUT',
      body: JSON.stringify(pipeline),
    });
  },

  async delete(id: string): Promise<{ message: string }> {
    return request<{ message: string }>(`/api/v1/pipelines/${id}`, {
      method: 'DELETE',
    });
  },

  // ─── Execution ──────────────────────────────────────

  async trigger(id: string): Promise<TriggerPipelineResponse> {
    return request<TriggerPipelineResponse>(`/api/v1/pipelines/${id}/trigger`, {
      method: 'POST',
    });
  },

  async stopInstance(instanceId: string): Promise<StopInstanceResponse> {
    return request<StopInstanceResponse>(`/api/v1/pipelines/instances/${instanceId}/stop`, {
      method: 'POST',
    });
  },

  async getInstanceStatus(instanceId: string): Promise<InstanceStatusResponse> {
    return request<InstanceStatusResponse>(`/api/v1/pipelines/instances/${instanceId}`);
  },

  async listRunningInstances(): Promise<{ instances: InstanceStatusResponse[]; total: number }> {
    return request(`/api/v1/pipelines/instances`);
  },

  // ─── History ────────────────────────────────────────

  async getExecutionHistory(
    pipelineId: string,
    page = 1,
    pageSize = 20
  ): Promise<ExecutionHistoryResponse> {
    return request<ExecutionHistoryResponse>(
      `/api/v1/pipelines/${pipelineId}/executions?page=${page}&page_size=${pageSize}`
    );
  },

  async getExecutionDetails(executionId: string): Promise<ExecutionRecord> {
    return request<ExecutionRecord>(`/api/v1/pipelines/executions/${executionId}`);
  },

  // ─── Import / Export ────────────────────────────────

  async exportAll(): Promise<ExportPipelinesResponse> {
    return request<ExportPipelinesResponse>('/api/v1/pipelines/export');
  },

  async exportByIds(ids: string[]): Promise<ExportPipelinesResponse> {
    return request<ExportPipelinesResponse>(
      `/api/v1/pipelines/export?ids=${ids.join(',')}`
    );
  },

  async importPipelines(
    pipelines: ExportedPipeline[],
    overwriteByName = false
  ): Promise<ImportPipelinesResponse> {
    return request<ImportPipelinesResponse>('/api/v1/pipelines/import', {
      method: 'POST',
      body: JSON.stringify({ pipelines, overwrite_by_name: overwriteByName }),
    });
  },
};
```

### React Hook Wrapper

```typescript
// src/hooks/usePipelineApi.ts

import { useState, useCallback } from 'react';
import { pipelineService } from '@/services/pipelineService';
import { PipelineDefinition, ListPipelinesResponse } from '@/types/pipeline';

export function usePipelines() {
  const [pipelines, setPipelines] = useState<PipelineDefinition[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchPipelines = useCallback(async (status?: string) => {
    setLoading(true);
    setError(null);
    try {
      const res = await pipelineService.list(status);
      setPipelines(res.pipelines ?? []);
      setTotal(res.total);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, []);

  return { pipelines, total, loading, error, fetchPipelines };
}

export function usePipelineMutations() {
  const [loading, setLoading] = useState(false);

  const createPipeline = useCallback(async (data: Parameters<typeof pipelineService.create>[0]) => {
    setLoading(true);
    try {
      return await pipelineService.create(data);
    } finally {
      setLoading(false);
    }
  }, []);

  const updatePipeline = useCallback(async (id: string, data: Parameters<typeof pipelineService.update>[1]) => {
    setLoading(true);
    try {
      return await pipelineService.update(id, data);
    } finally {
      setLoading(false);
    }
  }, []);

  const deletePipeline = useCallback(async (id: string) => {
    setLoading(true);
    try {
      return await pipelineService.delete(id);
    } finally {
      setLoading(false);
    }
  }, []);

  return { loading, createPipeline, updatePipeline, deletePipeline };
}
```

---

## 5. Route Configuration

Add these routes to `src/components/NebulaRouter.tsx`:

```tsx
// Add imports at the top alongside the other page imports
import PipelineListPage from "@/pages/pipelines/PipelineListPage";
import PipelineDesignerPage from "@/pages/pipelines/PipelineDesignerPage";

// Add inside <Routes> within the authenticated <Route> block, alongside existing routes:
<Route path="apps/external-pipelines" element={<PipelineListPage />} />
<Route path="apps/external-pipeline/:mode" element={<PipelineDesignerPage />} />
```

### URL Patterns

This follows the same singular/plural convention used by identities:

| Page | Identity Pattern | Pipeline Pattern |
|------|-----------------|-----------------|
| List | `/apps/identities` | `/apps/external-pipelines` |
| New | `/apps/identity/add` | `/apps/external-pipeline/add` |
| Edit | `/apps/identity/edit?id=123` | `/apps/external-pipeline/edit?id=PIPELINE_UUID` |

The `:mode` param is either `add` or `edit`. The edit page reads the pipeline ID from the `?id=` query parameter via `useSearchParams()`, same as `IdentityEditPage.tsx` does with `searchParams.get('id')`.

### Navigation Examples

```tsx
// From list page → create new
navigate('/apps/external-pipeline/add');

// From list page → edit existing
navigate(`/apps/external-pipeline/edit?id=${pipeline.id}`);

// From designer → back to list
navigate('/apps/external-pipelines');
```

---

## 6. Pipeline List Page

This page follows the exact pattern of `IdentityNewListPage.tsx`.

Since the pipeline backend is REST (not GraphQL), you cannot use `GlassGenericList` directly
(it requires a `DocumentNode` GraphQL query). Instead, build a custom list using the same
visual components: `PageLayout`, `BentoGrid`, `BentoCard`, `GlassButton`, `GlassStatusBadge`.

### Page Structure

```tsx
// src/pages/pipelines/PipelineListPage.tsx

import React, { useEffect, useState, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { Home, Grip, GitBranch, Plus, Play, Pause, Trash2, Edit3,
         Activity, CheckCircle, XCircle, AlertCircle, Clock } from 'lucide-react';
import { useBreadcrumb } from '@/hooks/useBreadcrumb';
import { useTranslation } from 'react-i18next';
import {
  GlassButton, GlassStatusBadge, PageLayout,
  spacing, borderRadius, useThemeColors, shadows,
  useToaster, GlassModal,
} from '@/ui';
import { BentoGrid, BentoCard } from '@/ui/layouts/BentoGrid';
import { GlassButtonStack } from '@/components/button/GlassButtonStack';
import { useModuleName } from '@/hooks/useAppPermission';
import { useAuth } from '@/contexts/AuthContext';
import { usePipelines, usePipelineMutations } from '@/hooks/usePipelineApi';
import { PipelineDefinition } from '@/types/pipeline';
import { gsap } from 'gsap';

const PipelineListPage: React.FC = () => {
  const themeColors = useThemeColors();
  const navigate = useNavigate();
  const { t } = useTranslation();
  const toast = useToaster();
  const moduleName = useModuleName(1);
  const { setModuleName } = useAuth();
  const { pipelines, total, loading, error, fetchPipelines } = usePipelines();
  const { deletePipeline } = usePipelineMutations();
  const [deleteTarget, setDeleteTarget] = useState<PipelineDefinition | null>(null);

  // Breadcrumbs — same pattern as IdentityNewListPage
  useBreadcrumb([
    { id: 'neb-home', label: t('nav.home'), icon: <Home size={16} />, path: '/home',
      onClick: () => navigate('/home') },
    { id: 'neb-app', label: t('nav.appLibrary'), icon: <Grip size={16} />, path: '/apps',
      onClick: () => { setModuleName?.(undefined); navigate('/apps', { state: { resetView: true } }); } },
    { id: 'neb-app-module', label: t(`${moduleName}`), icon: <Grip size={16} />, path: '/apps',
      onClick: () => navigate('/apps') },
    { id: 'neb-pipelines', label: 'Pipeline Management', icon: <GitBranch size={16} />,
      path: 'apps/external-pipelines', onClick: () => navigate('/apps/external-pipelines') },
  ], []);

  useEffect(() => { fetchPipelines(); }, [fetchPipelines]);

  // ─── KPI Counts ──────────────────────────────────────
  const activePipelines = pipelines.filter(p => p.status === 'active').length;
  const inactivePipelines = pipelines.filter(p => p.status === 'inactive').length;
  const scheduledPipelines = pipelines.filter(p => p.execution_mode === 'scheduled').length;
  const continuousPipelines = pipelines.filter(p => p.execution_mode === 'continuous').length;

  // ─── Delete Handler ──────────────────────────────────
  const handleDelete = async () => {
    if (!deleteTarget) return;
    try {
      await deletePipeline(deleteTarget.id);
      toast.success({ title: 'Deleted', message: `Pipeline "${deleteTarget.name}" deleted`, duration: 3000 });
      setDeleteTarget(null);
      fetchPipelines();
    } catch (err: any) {
      toast.error({ title: 'Delete Failed', message: err.message, duration: 5000 });
    }
  };

  return (
    <PageLayout
      title="Pipeline Management"
      subtitle="Design, manage, and monitor data pipelines"
      actions={
        <GlassButtonStack variant="dock" align="center" gap="md">
          <GlassButton
            id="NEB-pipeline-create"
            variant="primary"
            size="md"
            onClick={() => navigate('/apps/external-pipeline/add')}
            style={{ background: themeColors.gradients.primary, boxShadow: shadows.soft }}
          >
            <Plus size={18} style={{ marginRight: spacing.sm }} />
            New Pipeline
          </GlassButton>
        </GlassButtonStack>
      }
    >
      {/* ── KPI Cards ── */}
      {renderKPICards(total, activePipelines, inactivePipelines, scheduledPipelines, themeColors)}

      {/* ── Pipeline Table ── */}
      {renderPipelineTable(pipelines, loading, themeColors, navigate, setDeleteTarget)}

      {/* ── Delete Confirmation Modal ── */}
      <GlassModal
        isOpen={!!deleteTarget}
        onClose={() => setDeleteTarget(null)}
        title="Delete Pipeline"
        maxWidth="480px"
      >
        <p style={{ color: themeColors.text.secondary, marginBottom: spacing.xl }}>
          Are you sure you want to delete <strong>{deleteTarget?.name}</strong>?
          This action cannot be undone.
        </p>
        <GlassButtonStack variant="dock" align="right" gap="md">
          <GlassButton variant="secondary" size="md" onClick={() => setDeleteTarget(null)}>
            Cancel
          </GlassButton>
          <GlassButton variant="error" size="md" onClick={handleDelete}>
            <Trash2 size={16} style={{ marginRight: spacing.sm }} /> Delete
          </GlassButton>
        </GlassButtonStack>
      </GlassModal>
    </PageLayout>
  );
};

export default PipelineListPage;
```

### KPI Cards (BentoGrid)

Follow the same BentoGrid + BentoCard pattern from IdentityNewListPage:

```tsx
function renderKPICards(
  total: number, active: number, inactive: number, scheduled: number,
  themeColors: any
) {
  return (
    <div style={{ marginBottom: spacing.xl }}>
      <BentoGrid columns={12} gap="md">
        <BentoCard span={3} variant="primary" padding="sm" hoverable animated className="kpi-card">
          {/* Total Pipelines */}
          <div style={{ display: 'flex', alignItems: 'center', gap: spacing.md }}>
            <div style={{
              padding: spacing.md, borderRadius: borderRadius.md,
              background: 'linear-gradient(135deg, rgba(255,255,255,0.25), rgba(255,255,255,0.15))',
              backdropFilter: 'blur(10px)',
            }}>
              <GitBranch size={24} style={{ color: 'white' }} />
            </div>
            <div>
              <div style={{ fontSize: '2rem', fontWeight: 900, color: 'white' }}>
                {total}
              </div>
              <div style={{ fontSize: '0.875rem', color: 'rgba(255,255,255,0.9)' }}>
                Total Pipelines
              </div>
            </div>
          </div>
        </BentoCard>

        <BentoCard span={3} variant="secondary" padding="sm" hoverable animated className="kpi-card">
          {/* Active */}
          <div style={{ display: 'flex', alignItems: 'center', gap: spacing.md }}>
            <div style={{
              padding: spacing.md, borderRadius: borderRadius.md,
              background: 'linear-gradient(135deg, rgba(34,197,94,0.15), rgba(34,197,94,0.25))',
            }}>
              <CheckCircle size={24} style={{ color: '#22c55e' }} />
            </div>
            <div>
              <div style={{ fontSize: '2rem', fontWeight: 900, color: themeColors.text.primary }}>
                {active}
              </div>
              <div style={{ fontSize: '0.875rem', color: themeColors.text.secondary }}>Active</div>
            </div>
          </div>
        </BentoCard>

        <BentoCard span={3} variant="secondary" padding="sm" hoverable animated className="kpi-card">
          {/* Inactive */}
          <div style={{ display: 'flex', alignItems: 'center', gap: spacing.md }}>
            <div style={{
              padding: spacing.md, borderRadius: borderRadius.md,
              background: 'linear-gradient(135deg, rgba(234,179,8,0.15), rgba(234,179,8,0.25))',
            }}>
              <Pause size={24} style={{ color: '#eab308' }} />
            </div>
            <div>
              <div style={{ fontSize: '2rem', fontWeight: 900, color: themeColors.text.primary }}>
                {inactive}
              </div>
              <div style={{ fontSize: '0.875rem', color: themeColors.text.secondary }}>Inactive</div>
            </div>
          </div>
        </BentoCard>

        <BentoCard span={3} variant="secondary" padding="sm" hoverable animated className="kpi-card">
          {/* Scheduled */}
          <div style={{ display: 'flex', alignItems: 'center', gap: spacing.md }}>
            <div style={{
              padding: spacing.md, borderRadius: borderRadius.md,
              background: 'linear-gradient(135deg, rgba(99,102,241,0.15), rgba(99,102,241,0.25))',
            }}>
              <Clock size={24} style={{ color: '#6366f1' }} />
            </div>
            <div>
              <div style={{ fontSize: '2rem', fontWeight: 900, color: themeColors.text.primary }}>
                {scheduled}
              </div>
              <div style={{ fontSize: '0.875rem', color: themeColors.text.secondary }}>Scheduled</div>
            </div>
          </div>
        </BentoCard>
      </BentoGrid>
    </div>
  );
}
```

### Pipeline Table

Build a custom table using the glass morphism styling. Each row shows pipeline name, status, execution mode, component count, and action buttons.

```tsx
function renderPipelineTable(
  pipelines: PipelineDefinition[], loading: boolean, themeColors: any,
  navigate: (path: string) => void,
  setDeleteTarget: (p: PipelineDefinition) => void
) {
  if (loading) {
    return <div style={{ textAlign: 'center', padding: spacing['3xl'], color: themeColors.text.secondary }}>
      Loading pipelines...
    </div>;
  }

  if (pipelines.length === 0) {
    return <div style={{ textAlign: 'center', padding: spacing['3xl'], color: themeColors.text.secondary }}>
      No pipelines found. Create your first pipeline to get started.
    </div>;
  }

  return (
    <div style={{
      background: themeColors.glass.primary,
      borderRadius: borderRadius.lg,
      border: `1px solid ${themeColors.border.primary}`,
      overflow: 'hidden',
    }}>
      {/* Table Header */}
      <div style={{
        display: 'grid',
        gridTemplateColumns: '2fr 1fr 1fr 1fr 120px',
        padding: `${spacing.md} ${spacing.lg}`,
        borderBottom: `1px solid ${themeColors.border.primary}`,
        background: themeColors.glass.secondary,
      }}>
        {['Name', 'Status', 'Mode', 'Components', 'Actions'].map(h => (
          <div key={h} style={{
            fontSize: '0.75rem', fontWeight: 700, color: themeColors.text.secondary,
            textTransform: 'uppercase', letterSpacing: '0.5px',
          }}>{h}</div>
        ))}
      </div>

      {/* Table Rows */}
      {pipelines.map(pipeline => (
        <div
          key={pipeline.id}
          style={{
            display: 'grid',
            gridTemplateColumns: '2fr 1fr 1fr 1fr 120px',
            padding: `${spacing.md} ${spacing.lg}`,
            borderBottom: `1px solid ${themeColors.border.primary}`,
            cursor: 'pointer',
            transition: 'background 0.2s',
          }}
          onClick={() => navigate(`/apps/external-pipeline/edit?id=${pipeline.id}`)}
          onMouseEnter={e => (e.currentTarget.style.background = themeColors.glass.secondary)}
          onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}
        >
          {/* Name + Description */}
          <div>
            <div style={{ fontWeight: 700, color: themeColors.text.primary, fontSize: '0.875rem' }}>
              {pipeline.name}
            </div>
            <div style={{ color: themeColors.text.secondary, fontSize: '0.75rem', marginTop: spacing.xs }}>
              {pipeline.description || 'No description'}
            </div>
          </div>

          {/* Status Badge */}
          <div style={{ display: 'flex', alignItems: 'center' }}>
            <GlassStatusBadge
              status={pipeline.status === 'active' ? 'success' : 'warning'}
              label={pipeline.status}
              size="sm"
              icon={pipeline.status === 'active' ? <CheckCircle size={14} /> : <Pause size={14} />}
            />
          </div>

          {/* Execution Mode */}
          <div style={{ display: 'flex', alignItems: 'center', color: themeColors.text.primary, fontSize: '0.875rem' }}>
            {pipeline.execution_mode}
          </div>

          {/* Component Count */}
          <div style={{ display: 'flex', alignItems: 'center', color: themeColors.text.primary, fontSize: '0.875rem' }}>
            {pipeline.components?.length ?? 0} components
          </div>

          {/* Actions */}
          <div style={{ display: 'flex', gap: spacing.sm, alignItems: 'center' }}
               onClick={e => e.stopPropagation()}>
            <GlassButton variant="ghost" size="sm"
              onClick={() => navigate(`/apps/external-pipeline/edit?id=${pipeline.id}`)}>
              <Edit3 size={16} />
            </GlassButton>
            <GlassButton variant="ghost" size="sm"
              onClick={() => setDeleteTarget(pipeline)}>
              <Trash2 size={16} style={{ color: '#ef4444' }} />
            </GlassButton>
          </div>
        </div>
      ))}
    </div>
  );
}
```

---

## 7. Pipeline Designer Page (React Flow)

This is the main designer page. It follows the same pattern as `RulesDesignerPage.tsx` in the project.

The page layout:
- Left sidebar: Component palette (draggable nodes)
- Center: React Flow canvas
- Right sidebar: Configuration panel for selected node
- Top toolbar: Save, Run, Validate, Back buttons

### Page Component

```tsx
// src/pages/pipelines/PipelineDesignerPage.tsx

import React, { useState, useEffect, useCallback } from 'react';
import { useNavigate, useParams, useSearchParams } from 'react-router-dom';
import { ReactFlowProvider } from 'reactflow';
import { Home, Grip, GitBranch } from 'lucide-react';
import { useBreadcrumb } from '@/hooks/useBreadcrumb';
import { useTranslation } from 'react-i18next';
import { PageLayout, useThemeColors, useToaster } from '@/ui';
import { pipelineService } from '@/services/pipelineService';
import { PipelineDefinition, ComponentConfig, Connection } from '@/types/pipeline';
import PipelineDesigner from '@/components/pipeline/PipelineDesigner';

const PipelineDesignerPage: React.FC = () => {
  const { mode } = useParams<{ mode: string }>();
  const [searchParams] = useSearchParams();
  const pipelineId = searchParams.get('id');
  const isNew = mode === 'add';
  const navigate = useNavigate();
  const toast = useToaster();
  const themeColors = useThemeColors();
  const { t } = useTranslation();

  const [pipeline, setPipeline] = useState<PipelineDefinition | null>(null);
  const [loading, setLoading] = useState(!isNew);

  // Load existing pipeline
  useEffect(() => {
    if (!isNew && pipelineId) {
      setLoading(true);
      pipelineService.get(pipelineId)
        .then(setPipeline)
        .catch(err => {
          toast.error({ title: 'Load Failed', message: err.message, duration: 5000 });
          navigate('/apps/external-pipelines');
        })
        .finally(() => setLoading(false));
    }
  }, [isNew, pipelineId]);

  useBreadcrumb([
    { id: 'neb-home', label: t('nav.home'), icon: <Home size={16} />, path: '/home',
      onClick: () => navigate('/home') },
    { id: 'neb-pipelines', label: 'Pipelines', icon: <GitBranch size={16} />,
      path: '/apps/external-pipelines', onClick: () => navigate('/apps/external-pipelines') },
    { id: 'neb-pipeline-designer', label: isNew ? 'New Pipeline' : (pipeline?.name ?? 'Edit Pipeline'),
      icon: <GitBranch size={16} /> },
  ], [pipeline]);

  if (loading) {
    return <PageLayout title="Loading..."><div /></PageLayout>;
  }

  return (
    <ReactFlowProvider>
      <PipelineDesigner
        initialPipeline={pipeline}
        isNew={isNew}
        onSave={async (data) => {
          try {
            if (isNew) {
              const created = await pipelineService.create(data);
              toast.success({ title: 'Created', message: `Pipeline "${created.name}" created`, duration: 3000 });
              navigate(`/apps/external-pipeline/edit?id=${created.id}`);
            } else if (pipelineId) {
              await pipelineService.update(pipelineId, data);
              toast.success({ title: 'Saved', message: 'Pipeline updated', duration: 3000 });
            }
          } catch (err: any) {
            toast.error({ title: 'Save Failed', message: err.message, duration: 5000 });
          }
        }}
        onBack={() => navigate('/apps/external-pipelines')}
      />
    </ReactFlowProvider>
  );
};

export default PipelineDesignerPage;
```

### Designer Canvas Component

```tsx
// src/components/pipeline/PipelineDesigner.tsx

import React, { useState, useCallback, useMemo, useRef, useEffect } from 'react';
import ReactFlow, {
  Node, Edge, addEdge, Background, Controls, MiniMap,
  Connection as RFConnection, BackgroundVariant,
  useNodesState, useEdgesState, useReactFlow,
  NodeChange, EdgeChange,
} from 'reactflow';
import 'reactflow/dist/style.css';
import { Save, Play, ArrowLeft, CheckCircle, Settings } from 'lucide-react';
import { GlassButton, GlassInput, GlassDropdown, useThemeColors, spacing, borderRadius, shadows, useToaster } from '@/ui';
import { GlassButtonStack } from '@/components/button/GlassButtonStack';
import PipelineNodePalette from './PipelineNodePalette';
import PipelineConfigPanel from './PipelineConfigPanel';
import SourceNode from './nodes/SourceNode';
import ProcessorNode from './nodes/ProcessorNode';
import SinkNode from './nodes/SinkNode';
import {
  PipelineDefinition, CreatePipelineRequest, UpdatePipelineRequest,
  ComponentConfig, Connection as PipelineConnection, ComponentType,
  getComponentCategory, SOURCE_TYPES, SINK_TYPES,
} from '@/types/pipeline';

// Register custom node types
const nodeTypes = {
  source: SourceNode,
  processor: ProcessorNode,
  sink: SinkNode,
};

interface PipelineDesignerProps {
  initialPipeline: PipelineDefinition | null;
  isNew: boolean;
  onSave: (data: CreatePipelineRequest | UpdatePipelineRequest) => Promise<void>;
  onBack: () => void;
}

export default function PipelineDesigner({ initialPipeline, isNew, onSave, onBack }: PipelineDesignerProps) {
  const themeColors = useThemeColors();
  const toast = useToaster();
  const { screenToFlowPosition } = useReactFlow();

  // ─── Pipeline metadata ─────────────────────────────
  const [name, setName] = useState(initialPipeline?.name ?? '');
  const [description, setDescription] = useState(initialPipeline?.description ?? '');
  const [executionMode, setExecutionMode] = useState(initialPipeline?.execution_mode ?? 'scheduled');
  const [cronExpression, setCronExpression] = useState(initialPipeline?.cron_expression ?? '*/5 * * * *');
  const [status, setStatus] = useState(initialPipeline?.status ?? 'inactive');

  // ─── React Flow state ──────────────────────────────
  const [nodes, setNodes, onNodesChange] = useNodesState(
    initialPipeline ? pipelineToNodes(initialPipeline) : []
  );
  const [edges, setEdges, onEdgesChange] = useEdgesState(
    initialPipeline ? pipelineToEdges(initialPipeline) : []
  );
  const [selectedNode, setSelectedNode] = useState<Node | null>(null);

  // ─── Connection handler with validation ────────────
  const onConnect = useCallback((connection: RFConnection) => {
    const sourceNode = nodes.find(n => n.id === connection.source);
    const targetNode = nodes.find(n => n.id === connection.target);

    if (!sourceNode || !targetNode) return;

    // Validate: sinks cannot have outgoing connections
    if (SINK_TYPES.includes(sourceNode.data.componentType)) {
      toast.error({ title: 'Invalid Connection', message: 'Sink components cannot have outgoing connections', duration: 3000 });
      return;
    }
    // Validate: sources cannot have incoming connections
    if (SOURCE_TYPES.includes(targetNode.data.componentType)) {
      toast.error({ title: 'Invalid Connection', message: 'Source components cannot have incoming connections', duration: 3000 });
      return;
    }

    setEdges(eds => addEdge({
      ...connection,
      type: 'smoothstep',
      animated: true,
      style: { stroke: themeColors.gradients.accent, strokeWidth: 2 },
    }, eds));
  }, [nodes, setEdges, themeColors, toast]);

  // ─── Drag & Drop from palette ──────────────────────
  const onDragOver = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
  }, []);

  const onDrop = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    const componentType = event.dataTransfer.getData('application/pipeline-component') as ComponentType;
    if (!componentType) return;

    const position = screenToFlowPosition({ x: event.clientX, y: event.clientY });
    const category = getComponentCategory(componentType);
    const nodeId = `${componentType}-${Date.now()}`;

    const newNode: Node = {
      id: nodeId,
      type: category,  // 'source' | 'processor' | 'sink' — maps to custom node types
      position,
      data: {
        label: componentType.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase()),
        componentType,
        config: {
          id: nodeId,
          type: componentType,
          parameters: getDefaultParameters(componentType),
          retry_count: 0,
          retry_delay: 0,
          continue_on_error: false,
          timeout: 30000000000, // 30s in nanoseconds
        } as ComponentConfig,
      },
    };

    setNodes(nds => [...nds, newNode]);
  }, [screenToFlowPosition, setNodes]);

  // ─── Node selection ────────────────────────────────
  const onNodeClick = useCallback((_: React.MouseEvent, node: Node) => {
    setSelectedNode(node);
  }, []);

  const onPaneClick = useCallback(() => {
    setSelectedNode(null);
  }, []);

  // ─── Save handler ──────────────────────────────────
  const handleSave = useCallback(async () => {
    // Convert React Flow nodes/edges back to pipeline format
    const components: ComponentConfig[] = nodes.map(n => n.data.config);
    const connections: PipelineConnection[] = edges.map(e => ({
      source_component_id: e.source,
      target_component_id: e.target,
    }));

    // Client-side validation
    const hasSource = components.some(c => SOURCE_TYPES.includes(c.type));
    const hasSink = components.some(c => SINK_TYPES.includes(c.type));
    if (!hasSource) { toast.error({ title: 'Validation', message: 'Pipeline needs at least one source', duration: 3000 }); return; }
    if (!hasSink) { toast.error({ title: 'Validation', message: 'Pipeline needs at least one sink', duration: 3000 }); return; }
    if (!name.trim()) { toast.error({ title: 'Validation', message: 'Pipeline name is required', duration: 3000 }); return; }

    await onSave({
      name, description, execution_mode: executionMode,
      cron_expression: executionMode === 'scheduled' ? cronExpression : undefined,
      status, components, connections,
    });
  }, [nodes, edges, name, description, executionMode, cronExpression, status, onSave, toast]);

  // ─── Update node config when config panel changes ──
  const handleConfigChange = useCallback((nodeId: string, config: ComponentConfig) => {
    setNodes(nds => nds.map(n =>
      n.id === nodeId ? { ...n, data: { ...n.data, config } } : n
    ));
  }, [setNodes]);

  // ─── Delete node ───────────────────────────────────
  const handleDeleteNode = useCallback((nodeId: string) => {
    setNodes(nds => nds.filter(n => n.id !== nodeId));
    setEdges(eds => eds.filter(e => e.source !== nodeId && e.target !== nodeId));
    setSelectedNode(null);
  }, [setNodes, setEdges]);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: 'calc(100vh - 80px)' }}>
      {/* ── Top Toolbar ── */}
      <div style={{
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        padding: `${spacing.md} ${spacing.lg}`,
        background: themeColors.glass.primary,
        borderBottom: `1px solid ${themeColors.border.primary}`,
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: spacing.lg }}>
          <GlassButton variant="ghost" size="sm" onClick={onBack}>
            <ArrowLeft size={18} />
          </GlassButton>
          <GlassInput
            value={name} onChange={(v: any) => setName(String(v))}
            placeholder="Pipeline Name" style={{ minWidth: 250 }}
          />
          <GlassDropdown
            value={executionMode}
            onChange={(v: any) => setExecutionMode(v)}
            options={[
              { value: 'scheduled', label: 'Scheduled' },
              { value: 'continuous', label: 'Continuous' },
            ]}
          />
          {executionMode === 'scheduled' && (
            <GlassInput
              value={cronExpression} onChange={(v: any) => setCronExpression(String(v))}
              placeholder="Cron Expression" style={{ minWidth: 160 }}
            />
          )}
          <GlassDropdown
            value={status}
            onChange={(v: any) => setStatus(v)}
            options={[
              { value: 'inactive', label: 'Inactive' },
              { value: 'active', label: 'Active' },
            ]}
          />
        </div>
        <GlassButtonStack variant="dock" align="right" gap="md">
          <GlassButton variant="secondary" size="md" onClick={handleSave}>
            <Save size={16} style={{ marginRight: spacing.sm }} /> Save
          </GlassButton>
        </GlassButtonStack>
      </div>

      {/* ── Main Area: Palette | Canvas | Config Panel ── */}
      <div style={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
        {/* Left: Component Palette */}
        <PipelineNodePalette />

        {/* Center: React Flow Canvas */}
        <div style={{ flex: 1, position: 'relative' }}>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            onDragOver={onDragOver}
            onDrop={onDrop}
            onNodeClick={onNodeClick}
            onPaneClick={onPaneClick}
            nodeTypes={nodeTypes}
            fitView
            deleteKeyCode={['Backspace', 'Delete']}
            style={{ background: themeColors.glass.dark }}
          >
            <Background variant={BackgroundVariant.Dots} color="rgba(255,255,255,0.1)" gap={20} />
            <Controls style={{ background: themeColors.glass.secondary, borderRadius: borderRadius.md }} />
            <MiniMap
              style={{ background: themeColors.glass.secondary, borderRadius: borderRadius.md }}
              nodeColor={(n) => {
                if (n.type === 'source') return '#6366f1';
                if (n.type === 'processor') return '#966FB1';
                return '#22c55e';
              }}
            />
          </ReactFlow>
        </div>

        {/* Right: Config Panel (shown when a node is selected) */}
        {selectedNode && (
          <PipelineConfigPanel
            node={selectedNode}
            onConfigChange={handleConfigChange}
            onDelete={handleDeleteNode}
            onClose={() => setSelectedNode(null)}
          />
        )}
      </div>
    </div>
  );
}
```

### Helper: Convert Pipeline ↔ React Flow Nodes

```typescript
// Add these functions at the bottom of PipelineDesigner.tsx or in a separate utils file

function pipelineToNodes(pipeline: PipelineDefinition): Node[] {
  return pipeline.components.map((comp, index) => {
    const category = getComponentCategory(comp.type);
    // Auto-layout: arrange nodes in a grid
    const col = index % 3;
    const row = Math.floor(index / 3);
    return {
      id: comp.id,
      type: category,
      position: { x: 100 + col * 300, y: 100 + row * 200 },
      data: {
        label: comp.type.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase()),
        componentType: comp.type,
        config: comp,
      },
    };
  });
}

function pipelineToEdges(pipeline: PipelineDefinition): Edge[] {
  return pipeline.connections.map((conn, index) => ({
    id: `edge-${index}`,
    source: conn.source_component_id,
    target: conn.target_component_id,
    type: 'smoothstep',
    animated: true,
    style: { stroke: '#966FB1', strokeWidth: 2 },
  }));
}

function getDefaultParameters(type: ComponentType): Record<string, any> {
  const defaults: Record<string, Record<string, any>> = {
    http_get:           { url: 'https://api.example.com/data' },
    http_post:          { url: 'https://api.example.com/sink', content_type: 'application/json' },
    sql_query:          { connection_string: '', query: 'SELECT * FROM table_name' },
    csv_reader:         { file_path: '/path/to/data.csv', has_header: true, delimiter: ',', archive_on_read: false, move_on_error: false },
    kafka_consumer:     { brokers: ['localhost:9092'], topic: 'my-topic', group_id: 'my-group' },
    kafka_producer:     { brokers: ['localhost:9092'], topic: 'my-topic' },
    rabbitmq_consumer:  { connection_url: 'amqp://guest:guest@localhost:5672/', queue: 'my-queue' },
    rabbitmq_producer:  { connection_url: 'amqp://guest:guest@localhost:5672/', exchange: 'my-exchange', routing_key: '' },
    hl7_reader:         { file_path: '/path/to/messages.hl7' },
    tcp_read:           { host: 'localhost', port: 9000, mode: 'server' },
    tcp_write:          { host: 'localhost', port: 9001, mode: 'client' },
    python_code_block:  { code: '# Transform data here\nresult = data' },
    log:                { log_level: 'info' },
    attribute_update:   { mappings: [{ name: 'my_var', expression: '{{field_name}}' }] },
    log_sink:           { file_path: '/path/to/output.log', log_level: 'info', format: 'json', include_data: true, decode_payload: true },
  };
  return defaults[type] ?? {};
}
```

---

## 8. Component Palette (Node Palette)

This follows the same pattern as `src/components/rules/NodePalette.tsx` — a sidebar with draggable items grouped by category.

```tsx
// src/components/pipeline/PipelineNodePalette.tsx

import React from 'react';
import {
  Globe, Database, FileText, Radio, MessageSquare, Heart, Wifi,
  Code, FileOutput, GitMerge, Send, Upload, Terminal, BookOpen,
} from 'lucide-react';
import { useThemeColors, spacing, borderRadius } from '@/ui';
import { ComponentType } from '@/types/pipeline';

interface PaletteItem {
  type: ComponentType;
  label: string;
  icon: React.ReactNode;
  description: string;
}

const SOURCE_ITEMS: PaletteItem[] = [
  { type: 'http_get',           label: 'HTTP GET',           icon: <Globe size={16} />,         description: 'Fetch data from HTTP endpoint' },
  { type: 'sql_query',          label: 'SQL Query',          icon: <Database size={16} />,      description: 'Query a SQL database' },
  { type: 'csv_reader',         label: 'CSV Reader',         icon: <FileText size={16} />,      description: 'Read data from CSV file' },
  { type: 'kafka_consumer',     label: 'Kafka Consumer',     icon: <Radio size={16} />,         description: 'Consume from Kafka topic' },
  { type: 'rabbitmq_consumer',  label: 'RabbitMQ Consumer',  icon: <MessageSquare size={16} />, description: 'Consume from RabbitMQ queue' },
  { type: 'hl7_reader',         label: 'HL7 Reader',         icon: <Heart size={16} />,         description: 'Parse HL7 healthcare messages' },
  { type: 'tcp_read',           label: 'TCP Read',           icon: <Wifi size={16} />,          description: 'Read from TCP socket' },
];

const PROCESSOR_ITEMS: PaletteItem[] = [
  { type: 'python_code_block',  label: 'Python Code',        icon: <Code size={16} />,          description: 'Run Python transformation' },
  { type: 'log',                label: 'Log',                icon: <BookOpen size={16} />,      description: 'Log data passing through' },
  { type: 'attribute_update',   label: 'Attribute Update',   icon: <GitMerge size={16} />,      description: 'Map fields to {{variables}}' },
];

const SINK_ITEMS: PaletteItem[] = [
  { type: 'http_post',          label: 'HTTP POST',          icon: <Send size={16} />,          description: 'Send data to HTTP endpoint' },
  { type: 'kafka_producer',     label: 'Kafka Producer',     icon: <Upload size={16} />,        description: 'Publish to Kafka topic' },
  { type: 'rabbitmq_producer',  label: 'RabbitMQ Producer',  icon: <MessageSquare size={16} />, description: 'Publish to RabbitMQ exchange' },
  { type: 'tcp_write',          label: 'TCP Write',          icon: <Terminal size={16} />,      description: 'Write to TCP socket' },
  { type: 'log_sink',           label: 'Log Sink',           icon: <FileOutput size={16} />,    description: 'Write data to log file' },
];

const CATEGORY_COLORS = {
  source: '#6366f1',     // Indigo
  processor: '#966FB1',  // Purple (matches existing rules designer)
  sink: '#22c55e',       // Green
};

export default function PipelineNodePalette() {
  const themeColors = useThemeColors();

  const onDragStart = (event: React.DragEvent, componentType: ComponentType) => {
    event.dataTransfer.setData('application/pipeline-component', componentType);
    event.dataTransfer.effectAllowed = 'move';
  };

  const renderCategory = (title: string, items: PaletteItem[], color: string) => (
    <div style={{ marginBottom: spacing.lg }}>
      <h3 style={{
        fontSize: '0.6875rem', fontWeight: 700, color: themeColors.text.secondary,
        textTransform: 'uppercase', letterSpacing: '0.5px', marginBottom: spacing.sm,
        paddingLeft: spacing.sm,
      }}>{title}</h3>
      <div style={{ display: 'flex', flexDirection: 'column', gap: spacing.xs }}>
        {items.map(item => (
          <div
            key={item.type}
            draggable
            onDragStart={e => onDragStart(e, item.type)}
            style={{
              padding: `${spacing.sm} ${spacing.md}`,
              borderRadius: borderRadius.sm,
              border: `1px solid ${themeColors.border.primary}`,
              background: themeColors.glass.primary,
              cursor: 'grab',
              transition: 'all 0.2s',
            }}
            onMouseEnter={e => {
              e.currentTarget.style.background = themeColors.glass.secondary;
              e.currentTarget.style.borderColor = color;
            }}
            onMouseLeave={e => {
              e.currentTarget.style.background = themeColors.glass.primary;
              e.currentTarget.style.borderColor = themeColors.border.primary;
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: spacing.sm }}>
              <div style={{
                width: 28, height: 28, borderRadius: borderRadius.sm,
                background: `${color}30`, display: 'flex', alignItems: 'center', justifyContent: 'center',
                color: color,
              }}>
                {item.icon}
              </div>
              <div>
                <div style={{ fontSize: '0.8125rem', fontWeight: 600, color: themeColors.text.primary }}>
                  {item.label}
                </div>
                <div style={{ fontSize: '0.6875rem', color: themeColors.text.secondary }}>
                  {item.description}
                </div>
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );

  return (
    <div style={{
      width: 260, borderRight: `1px solid ${themeColors.border.primary}`,
      background: themeColors.glass.primary, overflowY: 'auto',
      padding: spacing.lg,
    }}>
      <h2 style={{
        fontSize: '0.875rem', fontWeight: 700, color: themeColors.text.primary,
        marginBottom: spacing.lg,
      }}>Components</h2>
      <p style={{ fontSize: '0.75rem', color: themeColors.text.secondary, marginBottom: spacing.lg }}>
        Drag components onto the canvas to build your pipeline.
      </p>

      {renderCategory('Sources', SOURCE_ITEMS, CATEGORY_COLORS.source)}
      {renderCategory('Processors', PROCESSOR_ITEMS, CATEGORY_COLORS.processor)}
      {renderCategory('Sinks', SINK_ITEMS, CATEGORY_COLORS.sink)}

      <div style={{
        marginTop: spacing.lg, padding: spacing.md,
        background: themeColors.glass.secondary, borderRadius: borderRadius.md,
        border: `1px solid ${themeColors.border.primary}`,
      }}>
        <p style={{ fontSize: '0.6875rem', color: themeColors.text.secondary }}>
          💡 <strong>Tip:</strong> Connect source → processor → sink.
          Use Attribute Update to create <code>{'{{variables}}'}</code> that downstream components can reference.
        </p>
      </div>
    </div>
  );
}
```

---

## 9. Custom React Flow Nodes

Three custom node types following the same pattern as `src/components/rules/nodes/RuleNode.tsx`.
Each node uses `Handle` from reactflow for connection points.

### Color Scheme

| Category  | Color   | Hex       |
|-----------|---------|-----------|
| Source    | Indigo  | `#6366f1` |
| Processor | Purple  | `#966FB1` |
| Sink      | Green   | `#22c55e` |

### Source Node

```tsx
// src/components/pipeline/nodes/SourceNode.tsx

import React from 'react';
import { Handle, Position, NodeProps } from 'reactflow';
import { useThemeColors, spacing, borderRadius } from '@/ui';

const COLOR = '#6366f1';

export default function SourceNode({ data, selected, id }: NodeProps) {
  const themeColors = useThemeColors();

  return (
    <div
      className={`glass-node ${selected ? 'ring-2 ring-white/50' : ''}`}
      style={{
        padding: spacing.md,
        borderRadius: borderRadius.md,
        background: themeColors.glass.primary,
        border: `2px solid ${selected ? COLOR : themeColors.border.primary}`,
        minWidth: 180,
        backdropFilter: 'blur(10px)',
        boxShadow: selected ? `0 0 20px ${COLOR}40` : 'none',
      }}
    >
      {/* Source nodes only have output handle (bottom) */}
      <Handle
        type="source"
        position={Position.Bottom}
        style={{ width: 10, height: 10, background: COLOR, border: '2px solid white' }}
      />

      <div style={{ display: 'flex', alignItems: 'center', gap: spacing.sm }}>
        <div style={{
          width: 32, height: 32, borderRadius: borderRadius.sm,
          background: `${COLOR}30`, display: 'flex', alignItems: 'center', justifyContent: 'center',
        }}>
          <div style={{ width: 8, height: 8, borderRadius: '50%', background: COLOR }} />
        </div>
        <div>
          <div style={{ fontSize: '0.75rem', fontWeight: 700, color: themeColors.text.primary }}>
            {data.label}
          </div>
          <div style={{ fontSize: '0.625rem', color: themeColors.text.secondary }}>
            Source
          </div>
        </div>
      </div>
    </div>
  );
}
```

### Processor Node

```tsx
// src/components/pipeline/nodes/ProcessorNode.tsx

import React from 'react';
import { Handle, Position, NodeProps } from 'reactflow';
import { useThemeColors, spacing, borderRadius } from '@/ui';

const COLOR = '#966FB1';

export default function ProcessorNode({ data, selected, id }: NodeProps) {
  const themeColors = useThemeColors();

  return (
    <div style={{
      padding: spacing.md, borderRadius: borderRadius.md,
      background: themeColors.glass.primary,
      border: `2px solid ${selected ? COLOR : themeColors.border.primary}`,
      minWidth: 180, backdropFilter: 'blur(10px)',
      boxShadow: selected ? `0 0 20px ${COLOR}40` : 'none',
    }}>
      {/* Processor has both input (top) and output (bottom) handles */}
      <Handle
        type="target"
        position={Position.Top}
        style={{ width: 10, height: 10, background: COLOR, border: '2px solid white' }}
      />
      <Handle
        type="source"
        position={Position.Bottom}
        style={{ width: 10, height: 10, background: COLOR, border: '2px solid white' }}
      />

      <div style={{ display: 'flex', alignItems: 'center', gap: spacing.sm }}>
        <div style={{
          width: 32, height: 32, borderRadius: borderRadius.sm,
          background: `${COLOR}30`, display: 'flex', alignItems: 'center', justifyContent: 'center',
        }}>
          <div style={{ width: 8, height: 8, borderRadius: '50%', background: COLOR }} />
        </div>
        <div>
          <div style={{ fontSize: '0.75rem', fontWeight: 700, color: themeColors.text.primary }}>
            {data.label}
          </div>
          <div style={{ fontSize: '0.625rem', color: themeColors.text.secondary }}>
            Processor
          </div>
        </div>
      </div>
    </div>
  );
}
```

### Sink Node

```tsx
// src/components/pipeline/nodes/SinkNode.tsx

import React from 'react';
import { Handle, Position, NodeProps } from 'reactflow';
import { useThemeColors, spacing, borderRadius } from '@/ui';

const COLOR = '#22c55e';

export default function SinkNode({ data, selected, id }: NodeProps) {
  const themeColors = useThemeColors();

  return (
    <div style={{
      padding: spacing.md, borderRadius: borderRadius.md,
      background: themeColors.glass.primary,
      border: `2px solid ${selected ? COLOR : themeColors.border.primary}`,
      minWidth: 180, backdropFilter: 'blur(10px)',
      boxShadow: selected ? `0 0 20px ${COLOR}40` : 'none',
    }}>
      {/* Sink nodes only have input handle (top) */}
      <Handle
        type="target"
        position={Position.Top}
        style={{ width: 10, height: 10, background: COLOR, border: '2px solid white' }}
      />

      <div style={{ display: 'flex', alignItems: 'center', gap: spacing.sm }}>
        <div style={{
          width: 32, height: 32, borderRadius: borderRadius.sm,
          background: `${COLOR}30`, display: 'flex', alignItems: 'center', justifyContent: 'center',
        }}>
          <div style={{ width: 8, height: 8, borderRadius: '50%', background: COLOR }} />
        </div>
        <div>
          <div style={{ fontSize: '0.75rem', fontWeight: 700, color: themeColors.text.primary }}>
            {data.label}
          </div>
          <div style={{ fontSize: '0.625rem', color: themeColors.text.secondary }}>
            Sink
          </div>
        </div>
      </div>
    </div>
  );
}
```

---

## 10. Component Configuration Panel

When a node is selected on the canvas, a right-side panel shows its configuration.
This uses `GlassDrawer` or a custom panel with `GlassInput`, `GlassDropdown`, etc.

The panel dynamically renders form fields based on the component type's required parameters.

```tsx
// src/components/pipeline/PipelineConfigPanel.tsx

import React, { useState, useEffect } from 'react';
import { Node } from 'reactflow';
import { Trash2, X, Settings } from 'lucide-react';
import {
  GlassButton, GlassInput, GlassDropdown,
  useThemeColors, spacing, borderRadius, shadows,
} from '@/ui';
import { ComponentConfig, ComponentType } from '@/types/pipeline';

interface PipelineConfigPanelProps {
  node: Node;
  onConfigChange: (nodeId: string, config: ComponentConfig) => void;
  onDelete: (nodeId: string) => void;
  onClose: () => void;
}

// Define which parameters each component type requires
const COMPONENT_PARAMS: Record<ComponentType, ParamDef[]> = {
  http_get:           [{ key: 'url', label: 'URL', type: 'text', required: true }],
  http_post:          [{ key: 'url', label: 'URL', type: 'text', required: true },
                       { key: 'content_type', label: 'Content Type', type: 'text', required: true }],
  sql_query:          [{ key: 'connection_string', label: 'Connection String', type: 'text', required: true },
                       { key: 'query', label: 'SQL Query', type: 'textarea', required: true }],
  csv_reader:         [{ key: 'file_path', label: 'File Path', type: 'text', required: true },
                       { key: 'has_header', label: 'Has Header Row', type: 'checkbox', required: false },
                       { key: 'delimiter', label: 'Delimiter', type: 'text', required: false },
                       { key: 'archive_on_read', label: 'Archive After Read', type: 'checkbox', required: false },
                       { key: 'move_on_error', label: 'Move to Error Folder on Failure', type: 'checkbox', required: false },
                       { key: 'archive_folder', label: 'Archive Folder', type: 'text', required: false },
                       { key: 'error_folder', label: 'Error Folder', type: 'text', required: false }],
  kafka_consumer:     [{ key: 'brokers', label: 'Brokers (comma-separated)', type: 'text', required: true },
                       { key: 'topic', label: 'Topic', type: 'text', required: true },
                       { key: 'group_id', label: 'Group ID', type: 'text', required: false }],
  kafka_producer:     [{ key: 'brokers', label: 'Brokers (comma-separated)', type: 'text', required: true },
                       { key: 'topic', label: 'Topic', type: 'text', required: true }],
  rabbitmq_consumer:  [{ key: 'connection_url', label: 'Connection URL', type: 'text', required: true },
                       { key: 'queue', label: 'Queue', type: 'text', required: true }],
  rabbitmq_producer:  [{ key: 'connection_url', label: 'Connection URL', type: 'text', required: true },
                       { key: 'exchange', label: 'Exchange', type: 'text', required: true },
                       { key: 'routing_key', label: 'Routing Key', type: 'text', required: false }],
  hl7_reader:         [{ key: 'file_path', label: 'File Path', type: 'text', required: true }],
  tcp_read:           [{ key: 'host', label: 'Host', type: 'text', required: true },
                       { key: 'port', label: 'Port', type: 'number', required: true },
                       { key: 'mode', label: 'Mode', type: 'select', required: true,
                         options: [{ value: 'server', label: 'Server' }, { value: 'client', label: 'Client' }] }],
  tcp_write:          [{ key: 'host', label: 'Host', type: 'text', required: true },
                       { key: 'port', label: 'Port', type: 'number', required: true },
                       { key: 'mode', label: 'Mode', type: 'select', required: true,
                         options: [{ value: 'server', label: 'Server' }, { value: 'client', label: 'Client' }] }],
  python_code_block:  [{ key: 'code', label: 'Python Code', type: 'textarea', required: true }],
  log:                [{ key: 'log_level', label: 'Log Level', type: 'select', required: true,
                         options: [{ value: 'debug', label: 'Debug' }, { value: 'info', label: 'Info' },
                                   { value: 'warn', label: 'Warn' }, { value: 'error', label: 'Error' }] }],
  attribute_update:   [{ key: 'mappings', label: 'Attribute Mappings', type: 'mappings', required: true }],
  log_sink:           [{ key: 'file_path', label: 'File Path', type: 'text', required: true },
                       { key: 'log_level', label: 'Log Level', type: 'select', required: true,
                         options: [{ value: 'debug', label: 'Debug' }, { value: 'info', label: 'Info' },
                                   { value: 'warn', label: 'Warn' }, { value: 'error', label: 'Error' }] },
                       { key: 'format', label: 'Format', type: 'select', required: false,
                         options: [{ value: 'json', label: 'JSON' }, { value: 'text', label: 'Text' }] },
                       { key: 'include_data', label: 'Include Payload Data', type: 'checkbox', required: false },
                       { key: 'decode_payload', label: 'Decode JSON Payload', type: 'checkbox', required: false }],
};

interface ParamDef {
  key: string;
  label: string;
  type: 'text' | 'number' | 'textarea' | 'select' | 'checkbox' | 'mappings';
  required: boolean;
  options?: { value: string; label: string }[];
}

export default function PipelineConfigPanel({ node, onConfigChange, onDelete, onClose }: PipelineConfigPanelProps) {
  const themeColors = useThemeColors();
  const config: ComponentConfig = node.data.config;
  const paramDefs = COMPONENT_PARAMS[config.type] ?? [];

  const updateParam = (key: string, value: any) => {
    const updated = {
      ...config,
      parameters: { ...config.parameters, [key]: value },
    };
    onConfigChange(node.id, updated);
  };

  const updateMeta = (field: keyof ComponentConfig, value: any) => {
    onConfigChange(node.id, { ...config, [field]: value });
  };

  return (
    <div style={{
      width: 320, borderLeft: `1px solid ${themeColors.border.primary}`,
      background: themeColors.glass.primary, overflowY: 'auto',
      padding: spacing.lg, display: 'flex', flexDirection: 'column',
    }}>
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: spacing.lg }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: spacing.sm }}>
          <Settings size={16} style={{ color: themeColors.text.secondary }} />
          <span style={{ fontSize: '0.875rem', fontWeight: 700, color: themeColors.text.primary }}>
            {node.data.label}
          </span>
        </div>
        <GlassButton variant="ghost" size="sm" onClick={onClose}><X size={16} /></GlassButton>
      </div>

      {/* Component ID (read-only) */}
      <div style={{ marginBottom: spacing.md }}>
        <label style={{ fontSize: '0.6875rem', color: themeColors.text.secondary, fontWeight: 600 }}>
          Component ID
        </label>
        <div style={{
          fontSize: '0.75rem', color: themeColors.text.primary, padding: spacing.sm,
          background: themeColors.glass.secondary, borderRadius: borderRadius.sm,
          fontFamily: 'monospace', marginTop: spacing.xs,
        }}>
          {config.id}
        </div>
      </div>

      {/* Dynamic Parameter Fields */}
      {paramDefs.map(param => (
        <div key={param.key} style={{ marginBottom: spacing.md }}>
          {param.type === 'mappings' ? (
            <AttributeMappingsEditor
              mappings={config.parameters.mappings ?? []}
              onChange={val => updateParam('mappings', val)}
              themeColors={themeColors}
            />
          ) : param.type === 'select' ? (
            <GlassDropdown
              label={param.label}
              value={String(config.parameters[param.key] ?? '')}
              onChange={(v: any) => updateParam(param.key, v)}
              options={param.options ?? []}
              required={param.required}
            />
          ) : param.type === 'checkbox' ? (
            <label style={{
              display: 'flex', alignItems: 'center', gap: spacing.sm,
              cursor: 'pointer', fontSize: '0.875rem', color: themeColors.text.primary,
            }}>
              <input
                type="checkbox"
                checked={!!config.parameters[param.key]}
                onChange={e => updateParam(param.key, e.target.checked)}
                style={{ width: 18, height: 18, accentColor: themeColors.gradients?.primary }}
              />
              {param.label}
            </label>
          ) : (
            <GlassInput
              label={param.label}
              value={config.parameters[param.key] ?? ''}
              onChange={(v: any) => updateParam(param.key, param.type === 'number' ? Number(v) : v)}
              type={param.type === 'textarea' ? 'textarea' : param.type === 'number' ? 'number' : 'text'}
              required={param.required}
              placeholder={param.label}
              rows={param.type === 'textarea' ? 6 : undefined}
            />
          )}
        </div>
      ))}

      {/* ── Advanced Settings ── */}
      <div style={{
        marginTop: spacing.lg, paddingTop: spacing.lg,
        borderTop: `1px solid ${themeColors.border.primary}`,
      }}>
        <h4 style={{ fontSize: '0.75rem', fontWeight: 700, color: themeColors.text.secondary, marginBottom: spacing.md }}>
          Advanced
        </h4>
        <GlassInput label="Retry Count" type="number" value={config.retry_count}
          onChange={(v: any) => updateMeta('retry_count', Number(v))} />
        <div style={{ height: spacing.sm }} />
        <GlassInput label="Timeout (seconds)" type="number"
          value={config.timeout / 1_000_000_000}
          onChange={(v: any) => updateMeta('timeout', Number(v) * 1_000_000_000)} />
      </div>

      {/* Delete Button */}
      <div style={{ marginTop: 'auto', paddingTop: spacing.xl }}>
        <GlassButton variant="error" size="md" onClick={() => onDelete(node.id)}
          style={{ width: '100%' }}>
          <Trash2 size={16} style={{ marginRight: spacing.sm }} /> Delete Component
        </GlassButton>
      </div>
    </div>
  );
}

// ─── Attribute Mappings Editor ────────────────────────────
// Special editor for the attribute_update component's mappings parameter.
// Each mapping has a "name" (the variable name) and "expression" (the value/template).

function AttributeMappingsEditor({
  mappings, onChange, themeColors,
}: {
  mappings: { name: string; expression: string }[];
  onChange: (mappings: { name: string; expression: string }[]) => void;
  themeColors: any;
}) {
  const addMapping = () => onChange([...mappings, { name: '', expression: '' }]);
  const removeMapping = (index: number) => onChange(mappings.filter((_, i) => i !== index));
  const updateMapping = (index: number, field: 'name' | 'expression', value: string) => {
    const updated = [...mappings];
    updated[index] = { ...updated[index], [field]: value };
    onChange(updated);
  };

  return (
    <div>
      <label style={{ fontSize: '0.6875rem', color: themeColors.text.secondary, fontWeight: 600 }}>
        Attribute Mappings
      </label>
      <p style={{ fontSize: '0.625rem', color: themeColors.text.secondary, marginBottom: spacing.sm }}>
        Define variables that downstream components can reference as <code>{'{{variable_name}}'}</code>.
        Expressions can reference upstream data fields like <code>{'{{field_name}}'}</code> or be static values.
      </p>
      {mappings.map((m, i) => (
        <div key={i} style={{
          display: 'flex', gap: spacing.xs, marginBottom: spacing.xs, alignItems: 'flex-start',
        }}>
          <GlassInput value={m.name} placeholder="Variable name"
            onChange={(v: any) => updateMapping(i, 'name', String(v))}
            style={{ flex: 1 }} />
          <GlassInput value={m.expression} placeholder="Expression / value"
            onChange={(v: any) => updateMapping(i, 'expression', String(v))}
            style={{ flex: 1 }} />
          <GlassButton variant="ghost" size="sm" onClick={() => removeMapping(i)}>
            <X size={14} />
          </GlassButton>
        </div>
      ))}
      <GlassButton variant="secondary" size="sm" onClick={addMapping} style={{ marginTop: spacing.xs }}>
        + Add Mapping
      </GlassButton>
    </div>
  );
}
```

---

## 11. Connection Validation Rules

These rules mirror the backend's `graph.go` validation. Enforce them in the `onConnect` callback.

| Source Type | Target Type | Allowed? |
|-------------|-------------|----------|
| Source      | Processor   | ✅ Yes   |
| Source      | Sink        | ✅ Yes   |
| Processor   | Processor   | ✅ Yes   |
| Processor   | Sink        | ✅ Yes   |
| Sink        | Any         | ❌ No (sinks cannot have outgoing connections) |
| Any         | Source      | ❌ No (sources cannot have incoming connections) |

Additional rules:
- Pipeline must have at least 1 source and 1 sink
- No cycles allowed (DAG only)
- All components must be connected (no orphan nodes)

The backend performs full validation on save (`POST`/`PUT`), so the frontend validation is for UX only.
If the backend rejects the pipeline, the error message from the API will explain why.

---

## 12. Pipeline Execution Controls

Add a toolbar button or panel to trigger/stop pipeline execution.

```tsx
// src/components/pipeline/PipelineExecutionPanel.tsx

import React, { useState, useEffect } from 'react';
import { Play, Square, RefreshCw, Clock } from 'lucide-react';
import { GlassButton, GlassStatusBadge, useThemeColors, spacing, borderRadius } from '@/ui';
import { pipelineService } from '@/services/pipelineService';
import { ExecutionRecord, InstanceStatus } from '@/types/pipeline';

interface PipelineExecutionPanelProps {
  pipelineId: string;
}

export default function PipelineExecutionPanel({ pipelineId }: PipelineExecutionPanelProps) {
  const themeColors = useThemeColors();
  const [instanceId, setInstanceId] = useState<string | null>(null);
  const [status, setStatus] = useState<InstanceStatus | null>(null);
  const [history, setHistory] = useState<ExecutionRecord[]>([]);
  const [loading, setLoading] = useState(false);

  // Load execution history
  useEffect(() => {
    pipelineService.getExecutionHistory(pipelineId, 1, 5)
      .then(res => setHistory(res.executions ?? []))
      .catch(() => {});
  }, [pipelineId]);

  // Poll instance status while running
  useEffect(() => {
    if (!instanceId || status !== 'running') return;
    const interval = setInterval(async () => {
      try {
        const res = await pipelineService.getInstanceStatus(instanceId);
        setStatus(res.status);
        if (res.status !== 'running') clearInterval(interval);
      } catch { clearInterval(interval); }
    }, 2000);
    return () => clearInterval(interval);
  }, [instanceId, status]);

  const handleTrigger = async () => {
    setLoading(true);
    try {
      const res = await pipelineService.trigger(pipelineId);
      setInstanceId(res.instance_id);
      setStatus('running');
    } catch (err: any) {
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const handleStop = async () => {
    if (!instanceId) return;
    try {
      await pipelineService.stopInstance(instanceId);
      setStatus('stopped');
    } catch (err: any) {
      console.error(err);
    }
  };

  const statusColor = (s: InstanceStatus) => {
    switch (s) {
      case 'running': return 'info';
      case 'completed': return 'success';
      case 'failed': return 'error';
      case 'stopped': return 'warning';
      default: return 'warning';
    }
  };

  return (
    <div style={{
      padding: spacing.lg, background: themeColors.glass.primary,
      borderRadius: borderRadius.md, border: `1px solid ${themeColors.border.primary}`,
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: spacing.md, marginBottom: spacing.lg }}>
        <GlassButton variant="primary" size="md" onClick={handleTrigger}
          disabled={loading || status === 'running'}>
          <Play size={16} style={{ marginRight: spacing.sm }} />
          {status === 'running' ? 'Running...' : 'Run Pipeline'}
        </GlassButton>
        {status === 'running' && (
          <GlassButton variant="warning" size="md" onClick={handleStop}>
            <Square size={16} style={{ marginRight: spacing.sm }} /> Stop
          </GlassButton>
        )}
        {status && <GlassStatusBadge status={statusColor(status)} label={status} size="sm" />}
      </div>

      {/* Recent Executions */}
      {history.length > 0 && (
        <div>
          <h4 style={{ fontSize: '0.75rem', fontWeight: 700, color: themeColors.text.secondary, marginBottom: spacing.sm }}>
            Recent Executions
          </h4>
          {history.map(exec => (
            <div key={exec.id} style={{
              display: 'flex', justifyContent: 'space-between', alignItems: 'center',
              padding: `${spacing.xs} 0`,
              borderBottom: `1px solid ${themeColors.border.primary}`,
            }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: spacing.sm }}>
                <Clock size={12} style={{ color: themeColors.text.secondary }} />
                <span style={{ fontSize: '0.75rem', color: themeColors.text.primary }}>
                  {new Date(exec.started_at).toLocaleString()}
                </span>
              </div>
              <GlassStatusBadge status={statusColor(exec.status)} label={exec.status} size="sm" />
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
```

---

## 13. React Flow Theming

Override React Flow's default styles to match the glass morphism theme.

Create a CSS file or add these styles to your pipeline designer component:

```css
/* src/components/pipeline/pipeline-designer.css */

/* Override React Flow default styles for glass morphism */
.react-flow__background {
  background: transparent !important;
}

.react-flow__controls {
  background: rgba(255, 255, 255, 0.08) !important;
  border: 1px solid rgba(255, 255, 255, 0.15) !important;
  border-radius: 12px !important;
  backdrop-filter: blur(10px) !important;
  box-shadow: 0 8px 32px rgba(0, 0, 0, 0.15) !important;
}

.react-flow__controls-button {
  background: transparent !important;
  border: none !important;
  color: rgba(255, 255, 255, 0.8) !important;
  fill: rgba(255, 255, 255, 0.8) !important;
}

.react-flow__controls-button:hover {
  background: rgba(255, 255, 255, 0.1) !important;
}

.react-flow__controls-button svg {
  fill: rgba(255, 255, 255, 0.8) !important;
}

.react-flow__minimap {
  background: rgba(255, 255, 255, 0.05) !important;
  border: 1px solid rgba(255, 255, 255, 0.15) !important;
  border-radius: 12px !important;
  backdrop-filter: blur(10px) !important;
}

.react-flow__edge-path {
  stroke: #966FB1 !important;
  stroke-width: 2 !important;
}

.react-flow__edge.animated .react-flow__edge-path {
  stroke-dasharray: 5 !important;
  animation: dashdraw 0.5s linear infinite !important;
}

@keyframes dashdraw {
  from { stroke-dashoffset: 10; }
  to { stroke-dashoffset: 0; }
}

.react-flow__connection-line {
  stroke: #966FB1 !important;
  stroke-width: 2 !important;
  stroke-dasharray: 5 5 !important;
}

.react-flow__handle {
  transition: all 0.2s ease !important;
}

.react-flow__handle:hover {
  transform: scale(1.3) !important;
  box-shadow: 0 0 8px rgba(150, 111, 177, 0.6) !important;
}

/* Selection box */
.react-flow__selection {
  background: rgba(150, 111, 177, 0.1) !important;
  border: 1px dashed rgba(150, 111, 177, 0.5) !important;
}
```

Import this CSS in `PipelineDesigner.tsx`:
```tsx
import './pipeline-designer.css';
```

---

## 14. State Management

The pipeline designer uses local React state (no global store needed). The state flow:

1. `PipelineDesignerPage` loads pipeline data from API (or starts empty for new)
2. Passes data to `PipelineDesigner` which converts it to React Flow nodes/edges
3. User edits on canvas update local `nodes`/`edges` state
4. On save, nodes/edges are converted back to `ComponentConfig[]` and `Connection[]`
5. The converted data is sent to the API via `pipelineService`

### Data Flow

```
API (PipelineDefinition)
  ↓ pipelineToNodes() / pipelineToEdges()
React Flow State (nodes[], edges[])
  ↓ user edits on canvas
React Flow State (updated)
  ↓ handleSave() → extract config from nodes, connections from edges
API Request (CreatePipelineRequest / UpdatePipelineRequest)
```

### Node Data Structure

Each React Flow node's `data` property contains:

```typescript
interface PipelineNodeData {
  label: string;              // Display name (e.g., "CSV Reader")
  componentType: ComponentType; // Backend type (e.g., "csv_reader")
  config: ComponentConfig;    // Full component config (id, type, parameters, retry, timeout)
}
```

When the user edits parameters in the config panel, `onConfigChange` updates the node's `data.config`.
When saving, `nodes.map(n => n.data.config)` extracts all component configs.

---

## 15. Backend API Reference

Base URL: `https://localhost:8080` (TLS, self-signed cert in dev)

All endpoints require `Authorization: Bearer <token>` header.

### Pipeline CRUD

| Method | Path | Description | Request Body | Response |
|--------|------|-------------|--------------|----------|
| `POST` | `/api/v1/pipelines` | Create pipeline | `CreatePipelineRequest` | `PipelineDefinition` |
| `GET` | `/api/v1/pipelines` | List all pipelines | — | `ListPipelinesResponse` |
| `GET` | `/api/v1/pipelines?status=active` | List filtered | — | `ListPipelinesResponse` |
| `GET` | `/api/v1/pipelines/{id}` | Get pipeline | — | `PipelineDefinition` |
| `PUT` | `/api/v1/pipelines/{id}` | Update pipeline | `UpdatePipelineRequest` | `PipelineDefinition` |
| `DELETE` | `/api/v1/pipelines/{id}` | Delete pipeline | — | `{ message: string }` |

### Execution

| Method | Path | Description | Response |
|--------|------|-------------|----------|
| `POST` | `/api/v1/pipelines/{id}/trigger` | Trigger execution | `TriggerPipelineResponse` |
| `POST` | `/api/v1/pipelines/instances/{id}/stop` | Stop instance | `StopInstanceResponse` |
| `GET` | `/api/v1/pipelines/instances/{id}` | Instance status | `InstanceStatusResponse` |
| `GET` | `/api/v1/pipelines/instances` | List running | `{ instances, total }` |

### History

| Method | Path | Description | Response |
|--------|------|-------------|----------|
| `GET` | `/api/v1/pipelines/{id}/executions?page=1&page_size=20` | Execution history | `ExecutionHistoryResponse` |
| `GET` | `/api/v1/pipelines/executions/{id}` | Execution details | `ExecutionRecord` |

### Import / Export

| Method | Path | Description | Response |
|--------|------|-------------|----------|
| `GET` | `/api/v1/pipelines/export` | Export all pipelines | `ExportPipelinesResponse` |
| `GET` | `/api/v1/pipelines/export?ids=id1,id2` | Export selected pipelines | `ExportPipelinesResponse` |
| `POST` | `/api/v1/pipelines/import` | Import pipelines from JSON | `ImportPipelinesResponse` |

### Error Response Format

```json
{
  "error": "Human-readable error message",
  "code": "VALIDATION_ERROR",
  "details": {}
}
```

Error codes: `INVALID_REQUEST`, `VALIDATION_ERROR`, `NOT_FOUND`, `EXECUTION_ERROR`, `INTERNAL_ERROR`

### CORS Note

If the React dev server runs on a different port (e.g., `localhost:5173`), you may need to add CORS
headers to the Go backend. Add this middleware in `internal/api/server.go`:

```go
import "github.com/go-chi/cors"

r.Use(cors.Handler(cors.Options{
    AllowedOrigins:   []string{"https://localhost:5173", "http://localhost:5173"},
    AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
    AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type"},
    AllowCredentials: true,
    MaxAge:           300,
}))
```

---

## 16. Component Type Reference

Complete reference for all 15 component types, their categories, and required parameters.

### Sources (7 types)

| Type | Required Parameters | Description |
|------|-------------------|-------------|
| `http_get` | `url` (string) | Fetches data from an HTTP GET endpoint |
| `sql_query` | `connection_string` (string), `query` (string) | Executes SQL query against a database |
| `csv_reader` | `file_path` (string) | Reads and parses a CSV file. Supports header detection, custom delimiters, and automatic file archiving/error handling |

#### csv_reader Optional Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `has_header` | bool | `true` | Whether the first row contains column headers |
| `delimiter` | string | `","` | Column delimiter character |
| `encoding` | string | `"utf-8"` | File encoding |
| `archive_on_read` | bool | `false` | Move file to archive folder after successful read |
| `move_on_error` | bool | `false` | Move file to error folder on read failure |
| `archive_folder` | string | `<file_dir>/archive` | Custom archive folder path (defaults to `archive/` next to the file) |
| `error_folder` | string | `<file_dir>/error` | Custom error folder path (defaults to `error/` next to the file) |

| Type | Required Parameters | Description |
|------|-------------------|-------------|
| `kafka_consumer` | `brokers` (string[]), `topic` (string) | Consumes messages from a Kafka topic |
| `rabbitmq_consumer` | `connection_url` (string), `queue` (string) | Consumes messages from a RabbitMQ queue |
| `hl7_reader` | `file_path` (string) | Parses HL7 v2.x healthcare messages |
| `tcp_read` | `host` (string), `port` (number), `mode` ("server"\|"client") | Reads data from TCP socket |

### Processors (3 types)

| Type | Required Parameters | Description |
|------|-------------------|-------------|
| `python_code_block` | `code` (string) | Executes Python code to transform data |
| `log` | `log_level` ("debug"\|"info"\|"warn"\|"error") | Logs data passing through (passthrough) |
| `attribute_update` | `mappings` (array of `{name, expression}`) | Creates `{{variables}}` from upstream data fields |

### Sinks (5 types)

| Type | Required Parameters | Description |
|------|-------------------|-------------|
| `http_post` | `url` (string), `content_type` (string) | Sends data to an HTTP POST endpoint |
| `kafka_producer` | `brokers` (string[]), `topic` (string) | Publishes messages to a Kafka topic |
| `rabbitmq_producer` | `connection_url` (string), `exchange` (string) | Publishes messages to a RabbitMQ exchange |
| `tcp_write` | `host` (string), `port` (number), `mode` ("server"\|"client") | Writes data to TCP socket |
| `log_sink` | `file_path` (string), `log_level` (string) | Writes data to a log file. Supports `{{variable}}` in `file_path` |

#### log_sink Optional Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `format` | string | `"text"` | Output format: `"json"` or `"text"` |
| `include_data` | bool | `true` | Include full data payload in log output. When `false`, only metadata is logged |
| `decode_payload` | bool | `true` | Decode JSON byte arrays to readable objects. When `false`, payload is logged as raw bytes |

### Attribute Update — Variable System

The `attribute_update` component is special. It creates named variables from upstream data:

```json
{
  "type": "attribute_update",
  "parameters": {
    "mappings": [
      { "name": "output_path", "expression": "/logs/{{department}}/output.log" },
      { "name": "log_level", "expression": "info" }
    ]
  }
}
```

Downstream components can then reference these as `{{output_path}}` and `{{log_level}}` in their parameters:

```json
{
  "type": "log_sink",
  "parameters": {
    "file_path": "{{output_path}}",
    "log_level": "{{log_level}}"
  }
}
```

Expressions support:
- Static values: `"info"`, `"/path/to/file.log"`
- Upstream field references: `{{field_name}}` (flat), `{{address.city}}` (nested)
- Built-in variables: `{{_timestamp}}`, `{{_trace_id}}`
- Chained references: later mappings can reference earlier ones

---

## 17. Sample Pipeline JSON

These are the exact JSON files the backend expects for `POST /api/v1/pipelines`.
Use these as a reference for the designer's save output. Both sample files are in the `docs/` folder of the backend project.

### Sample 1: CSV Reader to Log (`sample_pipeline_csv_to_log.json`)

```json
{
  "name": "CSV Reader to Log Pipeline",
  "description": "Reads employee CSV, uses attribute_update to set the log file path and log level, then log_sink references those as {{variables}}",
  "execution_mode": "scheduled",
  "cron_expression": "*/2 * * * *",
  "status": "active",
  "components": [
    {
      "id": "csv-reader-1",
      "type": "csv_reader",
      "parameters": {
        "file_path": "C:/Nebula.Conduit/docs/sample_data.csv"
      },
      "retry_count": 2,
      "retry_delay": 5000000000,
      "continue_on_error": false,
      "timeout": 30000000000
    },
    {
      "id": "attr-update-1",
      "type": "attribute_update",
      "parameters": {
        "mappings": [
          {
            "name": "log_output_path",
            "expression": "C:/Nebula.Conduit/logs/pipeline_output.log"
          },
          {
            "name": "log_output_level",
            "expression": "info"
          }
        ]
      },
      "retry_count": 0,
      "continue_on_error": true,
      "timeout": 10000000000
    },
    {
      "id": "log-sink-1",
      "type": "log_sink",
      "parameters": {
        "file_path": "{{log_output_path}}",
        "log_level": "{{log_output_level}}",
        "format": "json",
        "include_data": true,
        "decode_payload": true
      },
      "retry_count": 0,
      "continue_on_error": true,
      "timeout": 10000000000
    }
  ],
  "connections": [
    {
      "source_component_id": "csv-reader-1",
      "target_component_id": "attr-update-1"
    },
    {
      "source_component_id": "attr-update-1",
      "target_component_id": "log-sink-1"
    }
  ]
}
```

### Sample 2: CSV Reader with Archive (`sample_pipeline_csv_with_archive.json`)

This sample demonstrates the `archive_on_read` and `move_on_error` features of the CSV reader.
After a successful read, the source file is moved to an archive folder with a timestamp suffix.

```json
{
  "name": "CSV Reader with Archive to Log Pipeline",
  "description": "Reads employee CSV with archive support, uses attribute_update to set log path and options, then writes to log file",
  "execution_mode": "scheduled",
  "cron_expression": "*/2 * * * *",
  "status": "active",
  "components": [
    {
      "id": "csv-reader-1",
      "type": "csv_reader",
      "parameters": {
        "file_path": "C:/Nebula.Conduit/docs/sample_data.csv",
        "has_header": true,
        "archive_on_read": true,
        "move_on_error": true
      },
      "retry_count": 2,
      "retry_delay": 5000000000,
      "continue_on_error": false,
      "timeout": 30000000000
    },
    {
      "id": "attr-update-1",
      "type": "attribute_update",
      "parameters": {
        "mappings": [
          {
            "name": "log_output_path",
            "expression": "C:/tmp/pipeline_output.log"
          },
          {
            "name": "log_output_level",
            "expression": "info"
          },
          {
            "name": "log_format",
            "expression": "json"
          }
        ]
      },
      "retry_count": 0,
      "continue_on_error": true,
      "timeout": 10000000000
    },
    {
      "id": "log-sink-1",
      "type": "log_sink",
      "parameters": {
        "file_path": "{{log_output_path}}",
        "log_level": "{{log_output_level}}",
        "format": "{{log_format}}",
        "include_data": true,
        "decode_payload": true
      },
      "retry_count": 0,
      "continue_on_error": true,
      "timeout": 10000000000
    }
  ],
  "connections": [
    {
      "source_component_id": "csv-reader-1",
      "target_component_id": "attr-update-1"
    },
    {
      "source_component_id": "attr-update-1",
      "target_component_id": "log-sink-1"
    }
  ]
}
```

### Important Notes on Duration Fields

The backend uses Go's `time.Duration` which is in nanoseconds:
- `5000000000` = 5 seconds
- `10000000000` = 10 seconds
- `30000000000` = 30 seconds
- `60000000000` = 1 minute

In the config panel, show these as seconds to the user and convert:
- Display: `value / 1_000_000_000` (nanoseconds → seconds)
- Save: `value * 1_000_000_000` (seconds → nanoseconds)

### Streaming Execution Model

The backend executes pipelines using a streaming architecture based on Go channels. Understanding this helps when debugging execution status in the UI:

- Each component runs as a concurrent goroutine
- Source components produce data into a `<-chan Data` output channel
- Processor components read from an input channel, transform data, and write to an output channel
- Sink components consume from an input channel and write to their destination (file, HTTP, etc.)
- Connections between components are Go channels with a buffer size of 100
- When a source has multiple downstream targets, data is fanned out to all targets simultaneously
- The pipeline executor monitors all goroutines and reports completion/failure status
- Cancelling a pipeline instance cancels the shared context, which signals all goroutines to stop

This means pipeline execution is real-time and row-by-row (not batch). The execution status endpoint reflects the live state of all component goroutines.

---

## Appendix A: Project Component Reference

These are the existing Nebula.ng UI components used throughout this guide.
Import them from `@/ui` or their specific paths.

| Component | Import | Key Props |
|-----------|--------|-----------|
| `PageLayout` | `@/ui` | `title`, `subtitle`, `actions` |
| `BentoGrid` | `@/ui/layouts/BentoGrid` | `columns` (1\|2\|3\|4\|6\|12), `gap` ("sm"\|"md"\|"lg") |
| `BentoCard` | `@/ui/layouts/BentoGrid` | `span`, `variant` ("primary"\|"secondary"\|"accent"\|"elevated"), `hoverable`, `animated` |
| `GlassButton` | `@/ui` | `variant` ("primary"\|"secondary"\|"tertiary"\|"accent"\|"ghost"\|"success"\|"warning"\|"error"), `size` ("sm"\|"md"\|"lg"), `loading` |
| `GlassInput` | `@/ui` | `value`, `onChange`, `type` ("text"\|"password"\|"email"\|"number"\|"textarea"), `label`, `placeholder`, `required` |
| `GlassDropdown` | `@/ui` | `value`, `onChange`, `options` (`{value, label}[]`), `label` |
| `GlassModal` | `@/ui` | `isOpen`, `onClose`, `title`, `maxWidth` |
| `GlassDrawer` | `@/ui` | `isOpen`, `onClose`, `title`, `position` ("left"\|"right"), `width` |
| `GlassStatusBadge` | `@/ui` | `status` ("success"\|"warning"\|"error"\|"info"), `label`, `size`, `icon` |
| `GlassButtonStack` | `@/components/button/GlassButtonStack` | `variant` ("dock"), `align`, `gap` |
| `useThemeColors` | `@/ui` | Returns `themeColors` object with `glass`, `gradients`, `text`, `border` |
| `spacing` | `@/ui` | Token object: `xs`, `sm`, `md`, `lg`, `xl`, `2xl`, `3xl` |
| `borderRadius` | `@/ui` | Token object: `sm`, `md`, `lg`, `xl`, `full` |
| `shadows` | `@/ui` | Token object: `glass`, `glassHover`, `soft`, `modal` |
| `secureStorage` | `@/utils/secureStorage` | `.get('AUTH_TOKEN')` returns JWT token from sessionStorage |

## Appendix B: Existing React Flow Usage

The project already uses `reactflow@^11.11.4` in the Rules Designer (`src/components/rules/RulesDesigner.tsx`).
Follow the same patterns:

- Custom nodes use `Handle` from `reactflow` for connection points
- Drag-and-drop uses `event.dataTransfer.setData('application/reactflow', type)` pattern
- Node deletion dispatches custom events: `window.dispatchEvent(new CustomEvent('deleteNode', { detail: { nodeId } }))`
- The designer is wrapped in `<ReactFlowProvider>` at the page level
- Import `reactflow/dist/style.css` for base styles

## Appendix C: Configuration

The Conduit backend URL is configured in `common_settings.json` at the project root (not via environment variables).
This follows the same pattern used by all other services in the project.

Add the `CONDUIT_API_URL` key to `common_settings.json`:

```json
{
  "API_URL": "https://192.168.1.230/api",
  "SELF_API_URL": "/api",
  "DEPLOYMENT_ONPRIM": false,
  "VERTEXT_API_URL": "https://192.168.1.34:8000",
  "CONDUIT_API_URL": "https://localhost:8080",
  "PRODUCTION": false
}
```

The `pipelineService.ts` imports this file directly:

```typescript
import config from '../../common_settings.json';
const BASE_URL = config.CONDUIT_API_URL;
```

This is the same pattern used by:
- `src/apolloClient.ts` → `common_settings.API_URL`
- `src/pages/events/eventForensic/service/CameraService.ts` → `config.API_URL`
- `src/pages/events/eventForensic/service/VertexAIService.ts` → `configData.VERTEXT_API_URL`
- `src/services/NotificationService.ts` → `config.API_URL`

When deploying to production, update `CONDUIT_API_URL` in `common_settings.json` to point to the production Conduit backend.

---

## 18. Pipeline Import & Export

The backend provides dedicated import/export endpoints that use a portable JSON format.
This allows users to:
- Export pipelines from one environment and import into another (dev → staging → prod)
- Share pipeline definitions as files
- Back up pipeline configurations
- Bulk-create pipelines from a JSON file

### Backend API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/pipelines/export` | Export all pipelines |
| `GET` | `/api/v1/pipelines/export?ids=id1,id2` | Export specific pipelines by ID |
| `POST` | `/api/v1/pipelines/import` | Import pipelines from JSON |

### Export Format

The export format strips server-generated fields (id, created_at, updated_at) so the file is portable:

```json
{
  "version": "1.0",
  "exported_at": "2026-02-26T10:30:00Z",
  "pipelines": [
    {
      "name": "CSV Reader to Log Pipeline",
      "description": "Reads CSV and writes to log file",
      "execution_mode": "scheduled",
      "cron_expression": "*/5 * * * *",
      "status": "inactive",
      "components": [ ... ],
      "connections": [ ... ]
    }
  ],
  "total": 1
}
```

### Import Request

```json
{
  "pipelines": [
    {
      "name": "My Pipeline",
      "description": "...",
      "execution_mode": "scheduled",
      "cron_expression": "*/5 * * * *",
      "status": "inactive",
      "components": [ ... ],
      "connections": [ ... ]
    }
  ],
  "overwrite_by_name": false
}
```

When `overwrite_by_name` is `true`, if a pipeline with the same name already exists, it gets updated
instead of creating a duplicate. When `false`, duplicates are created with new IDs.

### Import Response

```json
{
  "imported": 2,
  "skipped": 0,
  "overwritten": 1,
  "errors": [],
  "message": "Import complete: 2 imported, 1 overwritten, 0 skipped"
}
```

### TypeScript Types

Add these to `src/types/pipeline.ts`:

```typescript
export interface ExportedPipeline {
  name: string;
  description: string;
  execution_mode: ExecutionMode;
  cron_expression?: string;
  status: PipelineStatus;
  components: ComponentConfig[];
  connections: Connection[];
}

export interface ExportPipelinesResponse {
  version: string;
  exported_at: string;
  pipelines: ExportedPipeline[];
  total: number;
}

export interface ImportPipelinesRequest {
  pipelines: ExportedPipeline[];
  overwrite_by_name: boolean;
}

export interface ImportPipelinesResponse {
  imported: number;
  skipped: number;
  overwritten: number;
  errors: string[];
  message: string;
}
```

### Service Methods

Add these to `src/services/pipelineService.ts`:

```typescript
  // ─── Import / Export ────────────────────────────────

  async exportAll(): Promise<ExportPipelinesResponse> {
    return request<ExportPipelinesResponse>('/api/v1/pipelines/export');
  },

  async exportByIds(ids: string[]): Promise<ExportPipelinesResponse> {
    return request<ExportPipelinesResponse>(
      `/api/v1/pipelines/export?ids=${ids.join(',')}`
    );
  },

  async importPipelines(
    pipelines: ExportedPipeline[],
    overwriteByName = false
  ): Promise<ImportPipelinesResponse> {
    return request<ImportPipelinesResponse>('/api/v1/pipelines/import', {
      method: 'POST',
      body: JSON.stringify({ pipelines, overwrite_by_name: overwriteByName }),
    });
  },
```

### Frontend Implementation

Add export/import buttons to the Pipeline List Page toolbar and the Pipeline Designer toolbar.

#### Export: Download as JSON File

```tsx
// Utility function — put in a shared utils file or inline
function downloadJson(data: any, filename: string) {
  const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}
```

##### Export All (List Page)

Add an "Export All" button to the `PipelineListPage` toolbar:

```tsx
import { Download, Upload } from 'lucide-react';

// Inside the <GlassButtonStack> in PipelineListPage actions:
<GlassButton
  id="NEB-pipeline-export-all"
  variant="secondary"
  size="md"
  onClick={async () => {
    try {
      const data = await pipelineService.exportAll();
      downloadJson(data, `pipelines-export-${new Date().toISOString().slice(0, 10)}.json`);
      toast.success({ title: 'Exported', message: `${data.total} pipeline(s) exported`, duration: 3000 });
    } catch (err: any) {
      toast.error({ title: 'Export Failed', message: err.message, duration: 5000 });
    }
  }}
>
  <Download size={18} style={{ marginRight: spacing.sm }} />
  Export All
</GlassButton>
```

##### Export Single (Designer Page)

Add an export button to the `PipelineDesigner` toolbar (only when editing an existing pipeline):

```tsx
// Inside the toolbar <GlassButtonStack> in PipelineDesigner, next to Save:
{!isNew && initialPipeline && (
  <GlassButton variant="ghost" size="md"
    onClick={async () => {
      try {
        const data = await pipelineService.exportByIds([initialPipeline.id]);
        downloadJson(data, `pipeline-${initialPipeline.name.replace(/\s+/g, '-').toLowerCase()}.json`);
        toast.success({ title: 'Exported', message: 'Pipeline exported', duration: 3000 });
      } catch (err: any) {
        toast.error({ title: 'Export Failed', message: err.message, duration: 5000 });
      }
    }}
  >
    <Download size={16} style={{ marginRight: spacing.sm }} /> Export
  </GlassButton>
)}
```

#### Import: Upload JSON File

##### Import Button + Hidden File Input

Add an "Import" button to the `PipelineListPage` toolbar:

```tsx
import { useRef } from 'react';

// Inside PipelineListPage component:
const fileInputRef = useRef<HTMLInputElement>(null);
const [importLoading, setImportLoading] = useState(false);

const handleImportFile = async (event: React.ChangeEvent<HTMLInputElement>) => {
  const file = event.target.files?.[0];
  if (!file) return;

  setImportLoading(true);
  try {
    const text = await file.text();
    const parsed = JSON.parse(text);

    // Accept both export format (has .pipelines array) and raw array
    let pipelines: ExportedPipeline[];
    if (Array.isArray(parsed)) {
      pipelines = parsed;
    } else if (parsed.pipelines && Array.isArray(parsed.pipelines)) {
      pipelines = parsed.pipelines;
    } else {
      // Single pipeline object
      pipelines = [parsed];
    }

    const result = await pipelineService.importPipelines(pipelines, false);
    toast.success({
      title: 'Import Complete',
      message: result.message,
      duration: 5000,
    });

    if (result.errors.length > 0) {
      result.errors.forEach(err => {
        toast.warning({ title: 'Import Warning', message: err, duration: 5000 });
      });
    }

    fetchPipelines(); // Refresh the list
  } catch (err: any) {
    toast.error({
      title: 'Import Failed',
      message: err.message || 'Invalid JSON file',
      duration: 5000,
    });
  } finally {
    setImportLoading(false);
    // Reset file input so the same file can be re-selected
    if (fileInputRef.current) fileInputRef.current.value = '';
  }
};

// In the JSX, add the hidden file input and the Import button:

{/* Hidden file input for import */}
<input
  ref={fileInputRef}
  type="file"
  accept=".json"
  style={{ display: 'none' }}
  onChange={handleImportFile}
/>

{/* Inside <GlassButtonStack> alongside Export All and New Pipeline: */}
<GlassButton
  id="NEB-pipeline-import"
  variant="secondary"
  size="md"
  loading={importLoading}
  onClick={() => fileInputRef.current?.click()}
>
  <Upload size={18} style={{ marginRight: spacing.sm }} />
  Import
</GlassButton>
```

#### Import with Overwrite Confirmation Modal

For a better UX, show a confirmation modal when importing that lets the user choose whether to
overwrite existing pipelines with the same name:

```tsx
const [importData, setImportData] = useState<ExportedPipeline[] | null>(null);
const [showImportModal, setShowImportModal] = useState(false);

// Modified handleImportFile — parse and show modal instead of importing directly:
const handleImportFile = async (event: React.ChangeEvent<HTMLInputElement>) => {
  const file = event.target.files?.[0];
  if (!file) return;

  try {
    const text = await file.text();
    const parsed = JSON.parse(text);

    let pipelines: ExportedPipeline[];
    if (Array.isArray(parsed)) {
      pipelines = parsed;
    } else if (parsed.pipelines && Array.isArray(parsed.pipelines)) {
      pipelines = parsed.pipelines;
    } else {
      pipelines = [parsed];
    }

    setImportData(pipelines);
    setShowImportModal(true);
  } catch {
    toast.error({ title: 'Invalid File', message: 'Could not parse JSON file', duration: 5000 });
  } finally {
    if (fileInputRef.current) fileInputRef.current.value = '';
  }
};

const executeImport = async (overwrite: boolean) => {
  if (!importData) return;
  setImportLoading(true);
  try {
    const result = await pipelineService.importPipelines(importData, overwrite);
    toast.success({ title: 'Import Complete', message: result.message, duration: 5000 });
    fetchPipelines();
  } catch (err: any) {
    toast.error({ title: 'Import Failed', message: err.message, duration: 5000 });
  } finally {
    setImportLoading(false);
    setShowImportModal(false);
    setImportData(null);
  }
};

// Import Confirmation Modal:
<GlassModal
  isOpen={showImportModal}
  onClose={() => { setShowImportModal(false); setImportData(null); }}
  title="Import Pipelines"
  maxWidth="520px"
>
  <p style={{ color: themeColors.text.secondary, marginBottom: spacing.md }}>
    Found <strong>{importData?.length ?? 0}</strong> pipeline(s) to import.
  </p>

  {/* Preview list */}
  <div style={{
    maxHeight: 200, overflowY: 'auto', marginBottom: spacing.lg,
    background: themeColors.glass.secondary, borderRadius: borderRadius.md,
    padding: spacing.md,
  }}>
    {importData?.map((p, i) => (
      <div key={i} style={{
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        padding: `${spacing.xs} 0`,
        borderBottom: i < (importData.length - 1) ? `1px solid ${themeColors.border.primary}` : 'none',
      }}>
        <span style={{ fontSize: '0.875rem', color: themeColors.text.primary, fontWeight: 600 }}>
          {p.name}
        </span>
        <GlassStatusBadge
          status={p.status === 'active' ? 'success' : 'warning'}
          label={p.execution_mode}
          size="sm"
        />
      </div>
    ))}
  </div>

  <p style={{ fontSize: '0.8125rem', color: themeColors.text.secondary, marginBottom: spacing.lg }}>
    If a pipeline with the same name already exists, choose whether to overwrite it or create a duplicate.
  </p>

  <GlassButtonStack variant="dock" align="right" gap="md">
    <GlassButton variant="secondary" size="md"
      onClick={() => { setShowImportModal(false); setImportData(null); }}>
      Cancel
    </GlassButton>
    <GlassButton variant="warning" size="md" loading={importLoading}
      onClick={() => executeImport(true)}>
      Import & Overwrite
    </GlassButton>
    <GlassButton variant="primary" size="md" loading={importLoading}
      onClick={() => executeImport(false)}>
      Import as New
    </GlassButton>
  </GlassButtonStack>
</GlassModal>
```

### Complete List Page Toolbar

Here's how the full toolbar looks with all buttons together:

```tsx
<GlassButtonStack variant="dock" align="center" gap="md">
  <GlassButton id="NEB-pipeline-export-all" variant="secondary" size="md"
    onClick={handleExportAll}>
    <Download size={18} style={{ marginRight: spacing.sm }} /> Export All
  </GlassButton>

  <GlassButton id="NEB-pipeline-import" variant="secondary" size="md"
    loading={importLoading} onClick={() => fileInputRef.current?.click()}>
    <Upload size={18} style={{ marginRight: spacing.sm }} /> Import
  </GlassButton>

  <GlassButtonDivider orientation="vertical" />

  <GlassButton id="NEB-pipeline-create" variant="primary" size="md"
    onClick={() => navigate('/apps/external-pipeline/add')}
    style={{ background: themeColors.gradients.primary, boxShadow: shadows.soft }}>
    <Plus size={18} style={{ marginRight: spacing.sm }} /> New Pipeline
  </GlassButton>
</GlassButtonStack>

{/* Hidden file input */}
<input ref={fileInputRef} type="file" accept=".json" style={{ display: 'none' }}
  onChange={handleImportFile} />
```

### curl Examples

```bash
# Export all pipelines
curl -k https://localhost:8080/api/v1/pipelines/export \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -o pipelines-export.json

# Export specific pipelines
curl -k "https://localhost:8080/api/v1/pipelines/export?ids=abc-123,def-456" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -o selected-pipelines.json

# Import pipelines (create as new)
curl -k -X POST https://localhost:8080/api/v1/pipelines/import \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d @pipelines-export.json

# Import with overwrite by name
curl -k -X POST https://localhost:8080/api/v1/pipelines/import \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{"pipelines": [...], "overwrite_by_name": true}'
```

### Postman Requests

Add these to the existing Postman collection under a new "Import/Export" folder:

| Request | Method | URL | Body |
|---------|--------|-----|------|
| Export All Pipelines | GET | `{{base_url}}/api/v1/pipelines/export` | — |
| Export Selected | GET | `{{base_url}}/api/v1/pipelines/export?ids={{pipeline_id}}` | — |
| Import Pipelines | POST | `{{base_url}}/api/v1/pipelines/import` | `{ "pipelines": [...], "overwrite_by_name": false }` |
