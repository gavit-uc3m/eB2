clear all
% close all
clc

%% Centrar datos

% Create rotation matrix
theta = 199; % Gaspar: 199, Juanjo: 166
R = [cosd(theta) -sind(theta); sind(theta) cosd(theta)];

x_0 = -3.77;
y_0 = +40.332;


%% Raw
datos = load('/Users/gavit/Documents/GitHub/eB2/test-lg-g4.mat');

data = datos.data;

v = [data(:,2).'; data(:,3).'];
v_rot = R*v;
    
figure, subplot(1,2,1), plot(v_rot(1,:)+x_0, v_rot(2,:)+y_0,'.r','MarkerSize',20), grid on
%axis([-0.2 1.2 -3.7 0.1])
handle(1)=gca;
subplot(1,2,1), plot_google_map


%% Clustered

datos_clus = load('/Users/gavit/Documents/GitHub/eB2/test-lg-g4_clustered.mat');

x_clus = datos_clus.x ;
y_clus = datos_clus.y ;


% figure,
subplot(1,2,2), 
for i=1:size(x_clus,1)
    i
    v = [x_clus{i, 1}; y_clus{i, 1}];
    v_rot = R*v;
    plot(v_rot(1,:)+x_0, v_rot(2,:)+y_0,'o','MarkerSize',10, 'linewidth', 5), grid on, hold on
    
end
handle(2)=gca;
subplot(1,2,2), plot_google_map

linkaxes(handle,'xy');


%% Raw
datos = load('/Users/gavit/Documents/GitHub/eB2/path1.mat');

data = datos.data;
time = data(:,1);
minutes = time/60;
hours = minutes/60;
days = hours/24;

v = [data(:,2).'; data(:,3).'];
v_rot = R*v;
    
figure, plot(v_rot(1,:)+x_0, v_rot(2,:)+y_0,'r','MarkerSize',20), grid on
hold on, plot(v_rot(1,1)+x_0, v_rot(2,1)+y_0,'b*','MarkerSize',20)
plot(v_rot(1,end)+x_0, v_rot(2,end)+y_0,'g*','MarkerSize',20)
%axis([-0.2 1.2 -3.7 0.1])
plot_google_map


%% Geo dist matrix

datos = load('/Users/gavit/Documents/GitHub/eB2/geo_dist_matrix.mat');

data = diag(datos.data,-1);
%surf(data);

wt = modwt(data,'haar',1);
mra = abs(modwtmra(wt,'haar'));

figure;
subplot(3,1,1)
plot(data); title('Distance to previous point');
subplot(3,1,2)
plot(mra(1,:)); title('Level 1 Details');
subplot(3,1,3)
plot(mra(2,:)); title('Level 2 Details');

AX = gca;
xlabel('Samples');


