## Requirements

### Requirement: Tailwind CSS v4 integration
The system SHALL use Tailwind CSS v4 for all styling via the Vite plugin. The configuration SHALL define design tokens for the Arneb brand including color palette, spacing scale, border radius, and font stack. Custom CSS SHALL be minimal — utility classes SHALL be the primary styling mechanism.

#### Scenario: Tailwind utility classes work
- **WHEN** a component uses Tailwind classes like `bg-primary text-white p-4 rounded-lg`
- **THEN** the corresponding styles are applied in both development and production builds

#### Scenario: Unused styles are purged
- **WHEN** the production build runs
- **THEN** the output CSS contains only the utility classes actually used in the source code

### Requirement: shadcn/ui component integration
The system SHALL use shadcn/ui as the component library, installed via the CLI into `web/src/components/ui/`. Components SHALL be source-owned (copied into the project, not imported from node_modules). The minimum set of components SHALL include: Button, Card, Table, Badge, Select, DropdownMenu, Tooltip, Separator, and Sheet (for mobile sidebar).

#### Scenario: Button component usage
- **WHEN** a developer imports `Button` from `@/components/ui/button`
- **THEN** the component renders an accessible button with variant support (default, destructive, outline, ghost)

#### Scenario: Component customization
- **WHEN** a developer needs to modify the Card component's default padding
- **THEN** they edit `web/src/components/ui/card.tsx` directly since components are source-owned

### Requirement: Dark and light mode toggle
The system SHALL support dark and light color modes. The mode SHALL be toggled via a button in the application header. The selected mode SHALL be persisted in `localStorage` under the key `arneb-theme`. On initial load, the system SHALL respect the user's OS preference via `prefers-color-scheme` media query if no stored preference exists. The mode SHALL be applied by adding/removing a `dark` class on the `<html>` element.

#### Scenario: Toggle to dark mode
- **WHEN** the user clicks the theme toggle button while in light mode
- **THEN** the UI switches to dark mode, `localStorage` stores `"dark"`, and the `<html>` element has class `dark`

#### Scenario: Persist theme preference
- **WHEN** the user selects dark mode and later reopens the application
- **THEN** the application loads in dark mode based on the `localStorage` value

#### Scenario: Respect OS preference on first visit
- **WHEN** a new user visits the application with OS set to dark mode and no `localStorage` value exists
- **THEN** the application renders in dark mode

### Requirement: Consistent color tokens
The system SHALL define semantic color tokens that adapt to light/dark mode. The tokens SHALL include: `background`, `foreground`, `card`, `primary`, `secondary`, `muted`, `accent`, `destructive`, and `border`. These tokens SHALL be defined as CSS custom properties in the Tailwind CSS theme and referenced via utility classes (e.g., `bg-primary`, `text-muted-foreground`).

#### Scenario: Light mode colors
- **WHEN** the application is in light mode
- **THEN** the background is light, text is dark, and cards have subtle shadows

#### Scenario: Dark mode colors
- **WHEN** the application is in dark mode
- **THEN** the background is dark, text is light, and cards have border-based separation instead of shadows

### Requirement: Responsive design
The system SHALL be fully responsive across desktop (1024px+), tablet (768px-1023px), and mobile (<768px) viewports. The layout SHALL adapt: desktop shows full sidebar + content, tablet shows collapsed sidebar + content, mobile shows hamburger menu overlay + full-width content. All data tables SHALL be horizontally scrollable on narrow viewports.

#### Scenario: Desktop layout
- **WHEN** the viewport width is 1280px
- **THEN** the full sidebar with labels is visible alongside the main content area

#### Scenario: Mobile layout
- **WHEN** the viewport width is 375px
- **THEN** the sidebar is hidden behind a hamburger menu, content takes full width, and tables scroll horizontally
