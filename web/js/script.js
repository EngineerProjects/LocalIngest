// Modern Code Ingest - JavaScript

// DOM Elements
const sidebarElement = document.querySelector('.sidebar');
const sidebarToggle = document.getElementById('sidebar-toggle');
const repoDirInput = document.getElementById('repo-path');
const validateBtn = document.getElementById('validate-btn');
const repoInfo = document.getElementById('repo-info');
const patternTypeSelect = document.getElementById('pattern-type');
const patternInput = document.getElementById('pattern-input');
const addPatternBtn = document.getElementById('add-pattern-btn');
const excludePatternsEl = document.getElementById('exclude-patterns');
const includePatternsEl = document.getElementById('include-patterns');
const includeSubfoldersEl = document.getElementById('include-subfolders');
const resetExcludeBtn = document.getElementById('reset-exclude-btn');
const clearIncludeBtn = document.getElementById('clear-include-btn');
const excludeRootFilesEl = document.getElementById('exclude-root-files');
const maxFileSizeEl = document.getElementById('max-file-size');
const maxFileSizeValueEl = document.getElementById('max-file-size-value');
const ingestBtn = document.getElementById('ingest-btn');
const tabBtns = document.querySelectorAll('.tab-btn');
const tabContents = document.querySelectorAll('.tab-content');
const downloadBtn = document.getElementById('download-btn');
const copyBtn = document.getElementById('copy-btn');
const summaryPlaceholder = document.getElementById('summary-placeholder');
const summaryContent = document.getElementById('summary-content');
const structurePlaceholder = document.getElementById('structure-placeholder');
const structureContent = document.getElementById('structure-content');
const filesPlaceholder = document.getElementById('files-placeholder');
const filesContent = document.getElementById('files-content');
const progressOverlay = document.getElementById('progress-overlay');
const progressBar = document.getElementById('progress-bar');
const progressMessage = document.getElementById('progress-message');
const progressDetails = document.getElementById('progress-details');
const toast = document.getElementById('toast');
const toastMessage = document.getElementById('toast-message');
const toastClose = document.getElementById('toast-close');

// State
let repoDirectory = null;
let directoryStructure = null;
let excludePatterns = [];
let includePatterns = [];
let includeSubfolders = [];
let currentAnalysisJob = null;
let analysisResult = null;

// Initialize
document.addEventListener('DOMContentLoaded', init);

function init() {
    // Load default exclude patterns
    fetchDefaultExcludes();
    
    // Set event listeners
    setupEventListeners();
    
    // Initialize UI state
    updateMaxFileSizeValue();
}

function setupEventListeners() {
    // Sidebar toggle
    sidebarToggle.addEventListener('click', toggleSidebar);
    
    // Repository validation
    validateBtn.addEventListener('click', validateRepository);
    repoDirInput.addEventListener('keyup', (e) => {
        if (e.key === 'Enter') validateRepository();
    });
    
    // Pattern management
    addPatternBtn.addEventListener('click', addPattern);
    patternInput.addEventListener('keyup', (e) => {
        if (e.key === 'Enter') addPattern();
    });
    resetExcludeBtn.addEventListener('click', resetExcludePatterns);
    clearIncludeBtn.addEventListener('click', clearIncludePatterns);
    
    // Range slider
    maxFileSizeEl.addEventListener('input', updateMaxFileSizeValue);
    
    // Start analysis
    ingestBtn.addEventListener('click', startAnalysis);
    
    // Tab switching
    tabBtns.forEach(btn => {
        btn.addEventListener('click', () => switchTab(btn.dataset.tab));
    });
    
    // Download and copy actions
    downloadBtn.addEventListener('click', downloadMarkdown);
    copyBtn.addEventListener('click', copyMarkdown);
    
    // Toast close button
    toastClose.addEventListener('click', hideToast);
}

// UI Functions
function toggleSidebar() {
    sidebarElement.classList.toggle('collapsed');
}

function switchTab(tabId) {
    // Update active tab button
    tabBtns.forEach(btn => {
        btn.classList.toggle('active', btn.dataset.tab === tabId);
    });
    
    // Update active tab content
    tabContents.forEach(content => {
        const isActive = content.id === `${tabId}-tab`;
        content.classList.toggle('active', isActive);
    });
}

function updateMaxFileSizeValue() {
    const value = maxFileSizeEl.value;
    maxFileSizeValueEl.textContent = `${value}KB`;
}

function showProgressOverlay(message, details) {
    progressMessage.textContent = message || 'Processing...';
    progressDetails.textContent = details || 'This may take a few minutes for larger repositories';
    progressBar.style.width = '0%';
    progressOverlay.classList.remove('hidden');
}

function hideProgressOverlay() {
    progressOverlay.classList.add('hidden');
}

function updateProgress(progress) {
    const percent = Math.min(Math.floor(progress * 100), 100);
    progressBar.style.width = `${percent}%`;
    
    // Update progress message based on the current progress
    if (progress < 0.2) {
        progressMessage.textContent = 'Initializing analysis...';
        progressDetails.textContent = 'Preparing to scan the repository';
    } else if (progress < 0.5) {
        progressMessage.textContent = 'Scanning repository...';
        progressDetails.textContent = 'Analyzing directory structure';
    } else if (progress < 0.8) {
        progressMessage.textContent = 'Processing files...';
        progressDetails.textContent = 'Reading and analyzing file contents';
    } else if (progress < 1) {
        progressMessage.textContent = 'Generating report...';
        progressDetails.textContent = 'Compiling analysis results';
    } else {
        progressMessage.textContent = 'Analysis complete!';
        progressDetails.textContent = 'Preparing to display results';
    }
}

function showToast(message, type = 'success') {
    // Set message
    toastMessage.textContent = message;
    
    // Set icon based on type
    const iconElement = toast.querySelector('.toast-icon i');
    if (type === 'success') {
        iconElement.className = 'fas fa-check-circle';
        toast.style.borderLeft = '4px solid var(--success)';
    } else if (type === 'error') {
        iconElement.className = 'fas fa-exclamation-circle';
        toast.style.borderLeft = '4px solid var(--danger)';
    } else if (type === 'warning') {
        iconElement.className = 'fas fa-exclamation-triangle';
        toast.style.borderLeft = '4px solid var(--warning)';
    } else if (type === 'info') {
        iconElement.className = 'fas fa-info-circle';
        toast.style.borderLeft = '4px solid var(--primary)';
    }
    
    // Show toast
    toast.classList.remove('hidden');
    
    // Auto-hide after 5 seconds
    setTimeout(hideToast, 5000);
}

function hideToast() {
    toast.classList.add('hidden');
}

// Pattern Management
function addPattern() {
    const pattern = patternInput.value.trim();
    if (!pattern) return;
    
    const type = patternTypeSelect.value;
    
    if (type === 'exclude') {
        if (!excludePatterns.includes(pattern)) {
            excludePatterns.push(pattern);
            patternInput.value = '';
            renderPatternLists();
            showToast(`Added exclude pattern: ${pattern}`, 'info');
        } else {
            showToast('This pattern is already in the exclude list', 'warning');
        }
    } else if (type === 'include') {
        // Check if it should be added to include patterns or subfolders
        if (pattern.endsWith('/') || !pattern.includes('*')) {
            const subfolder = pattern.replace(/\/$/, '');
            if (!includeSubfolders.includes(subfolder)) {
                includeSubfolders.push(subfolder);
                patternInput.value = '';
                renderPatternLists();
                showToast(`Added subfolder: ${subfolder}`, 'info');
            } else {
                showToast('This subfolder is already in the list', 'warning');
            }
        } else {
            if (!includePatterns.includes(pattern)) {
                includePatterns.push(pattern);
                patternInput.value = '';
                renderPatternLists();
                showToast(`Added include pattern: ${pattern}`, 'info');
            } else {
                showToast('This pattern is already in the include list', 'warning');
            }
        }
    }
}

function removePattern(type, index) {
    if (type === 'exclude') {
        const pattern = excludePatterns[index];
        excludePatterns.splice(index, 1);
        showToast(`Removed exclude pattern: ${pattern}`, 'info');
    } else if (type === 'include') {
        const pattern = includePatterns[index];
        includePatterns.splice(index, 1);
        showToast(`Removed include pattern: ${pattern}`, 'info');
    } else if (type === 'subfolder') {
        const pattern = includeSubfolders[index];
        includeSubfolders.splice(index, 1);
        showToast(`Removed subfolder: ${pattern}`, 'info');
    }
    
    renderPatternLists();
}

function resetExcludePatterns() {
    fetchDefaultExcludes();
    showToast('Reset exclude patterns to default', 'info');
}

function clearIncludePatterns() {
    includePatterns = [];
    includeSubfolders = [];
    renderPatternLists();
    showToast('Cleared all include patterns and subfolders', 'info');
}

function renderPatternLists() {
    // Render exclude patterns
    excludePatternsEl.innerHTML = '';
    
    if (excludePatterns.length === 0) {
        const emptyItem = document.createElement('li');
        emptyItem.innerHTML = '<span class="pattern-text">No exclude patterns defined</span>';
        emptyItem.style.color = 'var(--gray)';
        emptyItem.style.fontStyle = 'italic';
        excludePatternsEl.appendChild(emptyItem);
    } else {
        excludePatterns.forEach((pattern, index) => {
            const li = document.createElement('li');
            li.innerHTML = `
                <span class="pattern-text">${escapeHtml(pattern)}</span>
                <button class="pattern-remove" title="Remove pattern">
                    <i class="fas fa-times"></i>
                </button>
            `;
            li.querySelector('.pattern-remove').addEventListener('click', () => removePattern('exclude', index));
            excludePatternsEl.appendChild(li);
        });
    }
    
    // Render include patterns
    includePatternsEl.innerHTML = '';
    
    if (includePatterns.length === 0) {
        const emptyItem = document.createElement('li');
        emptyItem.innerHTML = '<span class="pattern-text">No include patterns defined</span>';
        emptyItem.style.color = 'var(--gray)';
        emptyItem.style.fontStyle = 'italic';
        includePatternsEl.appendChild(emptyItem);
    } else {
        includePatterns.forEach((pattern, index) => {
            const li = document.createElement('li');
            li.innerHTML = `
                <span class="pattern-text">${escapeHtml(pattern)}</span>
                <button class="pattern-remove" title="Remove pattern">
                    <i class="fas fa-times"></i>
                </button>
            `;
            li.querySelector('.pattern-remove').addEventListener('click', () => removePattern('include', index));
            includePatternsEl.appendChild(li);
        });
    }
    
    // Render include subfolders
    includeSubfoldersEl.innerHTML = '';
    
    if (includeSubfolders.length === 0) {
        const emptyItem = document.createElement('li');
        emptyItem.innerHTML = '<span class="pattern-text">No subfolders specified</span>';
        emptyItem.style.color = 'var(--gray)';
        emptyItem.style.fontStyle = 'italic';
        includeSubfoldersEl.appendChild(emptyItem);
    } else {
        includeSubfolders.forEach((pattern, index) => {
            const li = document.createElement('li');
            li.innerHTML = `
                <span class="pattern-text">${escapeHtml(pattern)}</span>
                <button class="pattern-remove" title="Remove pattern">
                    <i class="fas fa-times"></i>
                </button>
            `;
            li.querySelector('.pattern-remove').addEventListener('click', () => removePattern('subfolder', index));
            includeSubfoldersEl.appendChild(li);
        });
    }
}

// API Functions
async function fetchDefaultExcludes() {
    try {
        const response = await fetch('/api/default-excludes');
        
        if (!response.ok) {
            throw new Error('Failed to fetch default exclude patterns');
        }
        
        const data = await response.json();
        excludePatterns = [...data.exclude_patterns];
        renderPatternLists();
    } catch (error) {
        console.error('Error fetching default excludes:', error);
        showToast('Failed to fetch default exclude patterns', 'error');
    }
}

async function validateRepository() {
    const path = repoDirInput.value.trim();
    if (!path) {
        showToast('Please enter a repository path', 'warning');
        return;
    }
    
    try {
        showProgressOverlay('Validating repository...', 'Checking if the directory exists and is accessible');
        updateProgress(0.2);
        
        const response = await fetch('/api/validate-directory', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ path })
        });
        
        updateProgress(0.6);
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Invalid directory');
        }
        
        const data = await response.json();
        repoDirectory = data.full_path;
        
        // Create repository info card
        repoInfo.innerHTML = `
            <div class="repo-info-item">
                <span class="repo-info-label">Name:</span>
                <span class="repo-info-value">${escapeHtml(data.name)}</span>
            </div>
            <div class="repo-info-item">
                <span class="repo-info-label">Path:</span>
                <span class="repo-info-value">${escapeHtml(data.full_path)}</span>
            </div>
            <div class="repo-info-item">
                <span class="repo-info-label">Files:</span>
                <span class="repo-info-value">${data.approximate_file_count}</span>
            </div>
            <div class="repo-info-item">
                <span class="repo-info-label">Branch:</span>
                <span class="repo-info-value">${escapeHtml(data.branch)}</span>
            </div>
        `;
        repoInfo.classList.remove('hidden');
        
        // Fetch directory structure
        await fetchDirectoryStructure(path);
        
        updateProgress(1.0);
        setTimeout(hideProgressOverlay, 500);
        
        showToast('Repository validated successfully', 'success');
    } catch (error) {
        hideProgressOverlay();
        console.error('Error validating repository:', error);
        showToast(`Error: ${error.message}`, 'error');
    }
}

async function fetchDirectoryStructure(path) {
    try {
        const response = await fetch('/api/scan-structure', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ path })
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Failed to scan directory structure');
        }
        
        const data = await response.json();
        directoryStructure = data.directory_structure;
    } catch (error) {
        console.error('Error scanning directory structure:', error);
        showToast('Failed to scan directory structure', 'error');
    }
}

async function startAnalysis() {
    if (!repoDirectory) {
        showToast('Please validate a repository directory first', 'warning');
        return;
    }
    
    // Prepare analysis options
    const options = {
        directory: repoDirectory,
        exclude_patterns: excludePatterns,
        include_patterns: includePatterns,
        include_subfolders: includeSubfolders,
        exclude_root_files: excludeRootFilesEl.checked,
        max_file_size_kb: parseInt(maxFileSizeEl.value)
    };
    
    // Show progress overlay
    showProgressOverlay('Starting analysis...', 'Preparing to analyze the repository');
    updateProgress(0.05);
    
    try {
        // Start analysis job
        const response = await fetch('/api/analyze', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(options)
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Failed to start analysis');
        }
        
        const data = await response.json();
        currentAnalysisJob = data.job_id;
        
        // Poll for analysis status
        pollAnalysisStatus();
    } catch (error) {
        hideProgressOverlay();
        console.error('Error starting analysis:', error);
        showToast(`Error: ${error.message}`, 'error');
    }
}

async function pollAnalysisStatus() {
    if (!currentAnalysisJob) return;
    
    try {
        const response = await fetch(`/api/analysis/${currentAnalysisJob}`);
        
        if (!response.ok) {
            throw new Error('Failed to get analysis status');
        }
        
        const data = await response.json();
        
        // Update progress
        updateProgress(data.progress);
        
        if (data.status === 'running') {
            // Continue polling
            setTimeout(pollAnalysisStatus, 1000);
        } else if (data.status === 'completed') {
            // Fetch full result
            await fetchAnalysisResult();
        } else if (data.status === 'failed') {
            throw new Error(data.error || 'Analysis failed');
        }
    } catch (error) {
        hideProgressOverlay();
        console.error('Error polling analysis status:', error);
        showToast(`Error: ${error.message}`, 'error');
    }
}

async function fetchAnalysisResult() {
    if (!currentAnalysisJob) return;
    
    try {
        const response = await fetch(`/api/analysis/${currentAnalysisJob}/full`);
        
        if (!response.ok) {
            throw new Error('Failed to get analysis result');
        }
        
        // Get the full analysis result
        analysisResult = await response.json();
        
        // Update UI with results
        renderAnalysisResults();
        
        // Enable download and copy buttons
        downloadBtn.disabled = false;
        copyBtn.disabled = false;
        
        // Hide progress overlay and show success message
        hideProgressOverlay();
        showToast('Analysis completed successfully', 'success');
        
        // Switch to summary tab
        switchTab('summary');
    } catch (error) {
        hideProgressOverlay();
        console.error('Error fetching analysis result:', error);
        showToast(`Error: ${error.message}`, 'error');
    }
}

async function downloadMarkdown() {
    if (!currentAnalysisJob) {
        showToast('No analysis result available', 'warning');
        return;
    }
    
    try {
        const response = await fetch(`/api/analysis/${currentAnalysisJob}/markdown`);
        
        if (!response.ok) {
            throw new Error('Failed to get markdown content');
        }
        
        const data = await response.json();
        
        // Create a Blob and download link
        const blob = new Blob([data.markdown_content], { type: 'text/markdown' });
        const url = URL.createObjectURL(blob);
        
        const a = document.createElement('a');
        a.style.display = 'none';
        a.href = url;
        
        // Use repository name in filename if available
        let filename = 'code_ingest_report';
        if (analysisResult && analysisResult.summary && analysisResult.summary.repository) {
            const repo = Array.isArray(analysisResult.summary.repository) 
                ? analysisResult.summary.repository[0] 
                : analysisResult.summary.repository;
            
            // Extract last part of path
            const repoName = repo.split(/[\\/]/).pop();
            if (repoName) {
                filename = `${repoName}_analysis`;
            }
        }
        
        a.download = `${filename}_${new Date().toISOString().slice(0, 10)}.md`;
        
        document.body.appendChild(a);
        a.click();
        
        // Cleanup
        setTimeout(() => {
            URL.revokeObjectURL(url);
            document.body.removeChild(a);
        }, 100);
        
        showToast('Markdown report downloaded successfully', 'success');
    } catch (error) {
        console.error('Error downloading markdown:', error);
        showToast(`Error: ${error.message}`, 'error');
    }
}

async function copyMarkdown() {
    if (!currentAnalysisJob) {
        showToast('No analysis result available', 'warning');
        return;
    }
    
    try {
        const response = await fetch(`/api/analysis/${currentAnalysisJob}/markdown`);
        
        if (!response.ok) {
            throw new Error('Failed to get markdown content');
        }
        
        const data = await response.json();
        
        // Copy to clipboard
        await navigator.clipboard.writeText(data.markdown_content);
        showToast('Markdown content copied to clipboard', 'success');
    } catch (error) {
        console.error('Error copying markdown:', error);
        showToast(`Error: ${error.message}`, 'error');
    }
}

// Rendering Results
function renderAnalysisResults() {
    if (!analysisResult) return;
    
    // Render each view
    renderSummaryView();
    renderStructureView();
    renderFilesView();
}

function renderSummaryView() {
    const summary = analysisResult.summary;
    
    // Format repository name
    let repoName = 'Repository';
    if (Array.isArray(summary.repository)) {
        // Multiple repositories
        if (summary.repository.length === 1) {
            repoName = summary.repository[0].split(/[\\/]/).pop();
        } else {
            repoName = `${summary.repository.length} Repositories`;
        }
    } else {
        // Single repository
        repoName = summary.repository.split(/[\\/]/).pop();
    }
    
    // Format tokens in k
    const tokensInK = (summary.estimated_tokens / 1000).toFixed(1) + 'k';
    
    // Create summary HTML
    summaryContent.innerHTML = `
        <div class="summary-card">
            <div class="summary-header">
                <div class="summary-icon">
                    <i class="fas fa-code-branch"></i>
                </div>
                <h2 class="summary-title">${escapeHtml(repoName)}</h2>
            </div>
            
            <div class="summary-stats">
                <div class="stat-card">
                    <div class="stat-value">${summary.files_analyzed}</div>
                    <div class="stat-label">Files Analyzed</div>
                </div>
                
                <div class="stat-card">
                    <div class="stat-value">${tokensInK}</div>
                    <div class="stat-label">Estimated Tokens</div>
                </div>
                
                <div class="stat-card">
                    <div class="stat-value">${summary.analysis_time.toFixed(2)}s</div>
                    <div class="stat-label">Analysis Time</div>
                </div>
                
                <div class="stat-card">
                    <div class="stat-value">${escapeHtml(summary.branch)}</div>
                    <div class="stat-label">Branch</div>
                </div>
            </div>
        </div>
        
        <div class="summary-card">
            <h3 class="section-title">Analysis Configuration</h3>
            <div class="summary-config">
                <div class="config-item">
                    <strong>Exclude Patterns:</strong> ${excludePatterns.length > 0 ? escapeHtml(excludePatterns.join(', ')) : 'None'}
                </div>
                <div class="config-item">
                    <strong>Include Patterns:</strong> ${includePatterns.length > 0 ? escapeHtml(includePatterns.join(', ')) : 'None'}
                </div>
                <div class="config-item">
                    <strong>Include Subfolders:</strong> ${includeSubfolders.length > 0 ? escapeHtml(includeSubfolders.join(', ')) : 'None'}
                </div>
                <div class="config-item">
                    <strong>Exclude Root Files:</strong> ${excludeRootFilesEl.checked ? 'Yes' : 'No'}
                </div>
                <div class="config-item">
                    <strong>Max File Size:</strong> ${maxFileSizeEl.value}KB
                </div>
            </div>
        </div>
    `;
    
    // Show content and hide placeholder
    summaryPlaceholder.classList.add('hidden');
    summaryContent.classList.remove('hidden');
}

function renderStructureView() {
    const structure = analysisResult.directory_structure;
    const ingestedFiles = new Set(analysisResult.ingested_files || []);
    
    structureContent.innerHTML = '<div class="tree-view"></div>';
    const treeView = structureContent.querySelector('.tree-view');
    
    renderTreeNodes(treeView, structure, ingestedFiles);
    
    // Show content and hide placeholder
    structurePlaceholder.classList.add('hidden');
    structureContent.classList.remove('hidden');
    
    // Add folder toggle functionality
    const folderItems = structureContent.querySelectorAll('.tree-item.folder');
    folderItems.forEach(item => {
        item.addEventListener('click', (e) => {
            e.stopPropagation();
            const childrenContainer = item.nextElementSibling;
            if (childrenContainer && childrenContainer.classList.contains('tree-children')) {
                childrenContainer.classList.toggle('hidden');
                
                // Toggle folder icon
                const icon = item.querySelector('i');
                if (childrenContainer.classList.contains('hidden')) {
                    icon.className = 'fas fa-folder';
                } else {
                    icon.className = 'fas fa-folder-open';
                }
            }
        });
    });
}

function renderTreeNodes(parent, nodes, ingestedFiles, currentPath = '') {
    if (!nodes || nodes.length === 0) return;
    
    nodes.forEach(node => {
        const nodePath = currentPath ? `${currentPath}/${node.name}` : node.name;
        
        if (node.type === 'directory') {
            // Create folder item
            const folderItem = document.createElement('div');
            folderItem.className = 'tree-item folder';
            folderItem.innerHTML = `
                <i class="fas fa-folder${node.children && node.children.length > 0 ? '-open' : ''}"></i>
                <span>${escapeHtml(node.name)}</span>
                ${node.is_included === false ? '<span class="excluded-tag">(excluded)</span>' : ''}
            `;
            parent.appendChild(folderItem);
            
            // Create children container
            if (node.children && node.children.length > 0) {
                const childrenContainer = document.createElement('div');
                childrenContainer.className = 'tree-children';
                renderTreeNodes(childrenContainer, node.children, ingestedFiles, nodePath);
                parent.appendChild(childrenContainer);
            }
        } else {
            // Check if file was ingested
            const isIncluded = ingestedFiles.has(nodePath);
            
            // Create file item
            const fileItem = document.createElement('div');
            fileItem.className = `tree-item file${isIncluded ? ' included' : ''}`;
            
            // Determine file icon based on extension
            let fileIcon = 'fa-file';
            const ext = node.name.split('.').pop().toLowerCase();
            
            if (['js', 'ts', 'jsx', 'tsx'].includes(ext)) {
                fileIcon = 'fa-file-code';
            } else if (['html', 'htm', 'xml'].includes(ext)) {
                fileIcon = 'fa-file-code';
            } else if (['css', 'scss', 'sass', 'less'].includes(ext)) {
                fileIcon = 'fa-file-code';
            } else if (['py', 'rb', 'php', 'java', 'c', 'cpp', 'cs'].includes(ext)) {
                fileIcon = 'fa-file-code';
            } else if (['md', 'txt', 'rtf'].includes(ext)) {
                fileIcon = 'fa-file-alt';
            } else if (['json', 'yaml', 'yml', 'toml'].includes(ext)) {
                fileIcon = 'fa-file-code';
            } else if (['jpg', 'jpeg', 'png', 'gif', 'svg', 'webp'].includes(ext)) {
                fileIcon = 'fa-file-image';
            }
            
            fileItem.innerHTML = `
                <i class="fas ${fileIcon}"></i>
                <span>${escapeHtml(node.name)}</span>
                ${node.size ? `<small>(${formatFileSize(node.size)})</small>` : ''}
                ${isIncluded ? '<span class="included-tag">✓</span>' : ''}
                ${node.is_included === false ? '<span class="excluded-tag">(excluded)</span>' : ''}
            `;
            
            parent.appendChild(fileItem);
        }
    });
}

function renderFilesView() {
    const fileContents = analysisResult.file_contents;
    
    if (!fileContents || fileContents.length === 0) {
        filesContent.innerHTML = `
            <div class="empty-state">
                <div class="empty-icon">
                    <i class="fas fa-file-alt"></i>
                </div>
                <h3 class="empty-title">No Files Included</h3>
                <p class="empty-text">No files were included in the analysis. Try adjusting your include/exclude patterns or max file size.</p>
            </div>
        `;
    } else {
        filesContent.innerHTML = '';
        
        // Show file count info
        const fileCountInfo = document.createElement('div');
        fileCountInfo.className = 'file-count-info';
        fileCountInfo.innerHTML = `
            <p>Showing ${Math.min(fileContents.length, 5)} of ${fileContents.length} files</p>
        `;
        filesContent.appendChild(fileCountInfo);
        
        // Display first 5 files
        const filesToShow = fileContents.slice(0, 5);
        
        filesToShow.forEach(file => {
            const fileCard = document.createElement('div');
            fileCard.className = 'file-card';
            
            // Determine file type label based on extension
            const ext = file.path.split('.').pop().toLowerCase();
            let fileTypeLabel = ext;
            
            if (['js', 'ts', 'jsx', 'tsx'].includes(ext)) {
                fileTypeLabel = 'JavaScript';
            } else if (['html', 'htm'].includes(ext)) {
                fileTypeLabel = 'HTML';
            } else if (['css', 'scss', 'sass', 'less'].includes(ext)) {
                fileTypeLabel = 'Stylesheet';
            } else if (['py'].includes(ext)) {
                fileTypeLabel = 'Python';
            } else if (['md'].includes(ext)) {
                fileTypeLabel = 'Markdown';
            } else if (['json'].includes(ext)) {
                fileTypeLabel = 'JSON';
            }
            
            fileCard.innerHTML = `
                <div class="file-header">
                    <div class="file-path">${escapeHtml(file.path)}</div>
                    <div class="file-meta">
                        <div class="file-size">${formatFileSize(file.size)}</div>
                        <div class="file-type">${fileTypeLabel}</div>
                    </div>
                </div>
                <div class="file-content">
                    <pre><code>${escapeHtml(file.content)}</code></pre>
                </div>
            `;
            
            filesContent.appendChild(fileCard);
        });
        
        // Show message if there are more files
        if (fileContents.length > 5) {
            const moreFiles = document.createElement('div');
            moreFiles.className = 'more-files-message';
            moreFiles.innerHTML = `
                <p>${fileContents.length - 5} more files not shown. Download the full report to see all files.</p>
            `;
            filesContent.appendChild(moreFiles);
        }
    }
    
    // Show content and hide placeholder
    filesPlaceholder.classList.add('hidden');
    filesContent.classList.remove('hidden');
}

// Helper Functions
function formatFileSize(bytes) {
    if (bytes < 1024) {
        return `${bytes} B`;
    } else if (bytes < 1024 * 1024) {
        return `${(bytes / 1024).toFixed(1)} KB`;
    } else {
        return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
    }
}

function escapeHtml(unsafe) {
    return unsafe
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}