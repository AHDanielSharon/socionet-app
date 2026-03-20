export const canEditPost = (post: any, userId?: string) => post.user_id === userId;
export const canDeletePost = (post: any, userId?: string) => post.user_id === userId;
export const canDeleteComment = (comment: any, post: any, userId?: string) => comment.user_id === userId || post.user_id === userId;
export const canManageCommunity = (member: any) => ['admin','owner','moderator'].includes(member?.role);
export const canViewPrivateProfile = (profile: any, viewer: any) => !profile.is_private || profile.id === viewer?.id || profile.is_following;
