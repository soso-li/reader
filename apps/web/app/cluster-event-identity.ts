export type ClusterEventIdentity = {
  event_uid: string | null;
  current_revision_uid: string | null;
  seen_revision_uid: string | null;
  current_revision_differs_from_seen: boolean;
  has_material_update: boolean;
  material_update_revision_uid: string | null;
  uninterested: boolean;
  uninterested_reason: string | null;
  uninterested_note: string | null;
  uninterested_at: string | null;
};
