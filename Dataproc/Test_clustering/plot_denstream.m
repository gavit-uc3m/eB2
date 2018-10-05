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

figure, hold on,
datos = load('/Users/gavit/Documents/GitHub/eB2/Dataproc/Test_clustering/test-lg-g4.mat');

data = datos.data;

v = [data(:,1).'; data(:,2).'];
v_rot = R*v;
plot(v_rot(1,:)+x_0, v_rot(2,:)+y_0,'r*','MarkerSize',10)


%datos = load('/Users/gavit/Documents/GitHub/eB2/Cluster/DensStream/pruebas7_sec_clusters.mat');
datos = load('/Users/gavit/Documents/GitHub/eB2/Dataproc/Test_clustering/cloud_g4_clusters.mat');

data = datos.clusters;
dia1 = data;%{end};
%dia2 = data{500};

t = linspace(0, 2*pi, 20);
x = cos(t); y = sin(t);


for i = length(dia1):-1:1
    
    a = R*[dia1(i,1); dia1(i,2)];
    plot(a(1)+x_0, a(2)+y_0, 'b*','MarkerSize',10), hold on
    plot(a(1)+1.4*x*dia1(i,3)+x_0, a(2)+1.4*y*dia1(i,3)+y_0, 'b.-','linewidth', 4)
    
%     a = R*[dia2(i,1); dia2(i,2)];
%     plot(a(1)+x_0, a(2)+y_0, 'rx'), hold on
%     plot(a(1)+x*dia2(i,3)+x_0, a(2)+y*dia2(i,3)+y_0, 'r--')

end

plot_google_map;
axis equal
%axis([-10 3 36 44])
    

% subplot(1,2,1), plot(w(1,:), w(2,:), 'b*','MarkerSize',20)

% % clusters con radio
% centro = R*[-0.0756002; 0.400876]+[x_0; y_0];
% radio = 4.360701127e-05
% t = linspace(0, 2*pi, 1000);
% x = radio*cos(t)+centro(1,:);
% y = radio*sin(t)+centro(2,:);
% subplot(1,2,1), plot(x, y, 'b.-','MarkerSize',20)
