clear all
close all
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




